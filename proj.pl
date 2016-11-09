#!/usr/bin/perl

our $OS_win = ($^O eq "MSWin32") ? 1 : 0;
our $fail=0;

use strict;
use warnings;
use IO::Socket;
use MBclient;
use parsec_crc_ether;
use DBI;
use DBD::mysql;

my $d_bg=0;
$| = 1;

##################### reading PLC init ################################# 
# for print frame and debug string : uncomment this line
#$m->{debug} = 1 if $d_bg;

#################### end of configurations #############################################

if (!$OS_win) {
  $SIG{'TERM'} = 'END_handler';
  $SIG{'ABRT'} = 'END_handler';
  $SIG{'HUP'} = 'END_handler';
}
my $p;
my $w;
my $m;
my $dbh;
DBConnect('localhost','WeightsDb','ws','12345');


#########
my $prv_tag='0';
my $cur_tag='0';
my $ch1_tag='0';
my $bits;
my $tare=0;
my @mmm;
my $dg;
my $prv_w='0.00';
my $w_ok;
my @res=();
ExecSql("INSERT INTO `Logs` (`TimeKey`, `Pid`, `Comments`) VALUES (now(),$$,'Start proj.pl');");
while (not $fail) {

# 1.Read ID /set tare/
  ReadTag(1,2,0x62); # Addr, channelNum, Cmd
  $p->recv($dg,42);
  $p->close;
# check substr($dg,8,1)==0 (NO_ERR)

  $cur_tag=unpack( 'N',reverse(substr($dg,10,ord(substr($dg,9,1)))));
print "Ch2\tCurTag\t$cur_tag; PrvTag\t$prv_tag\n" if $d_bg;
if ( $d_bg and ($cur_tag eq '0')) {$prv_tag=$cur_tag;}
  ExecSql("INSERT INTO `Logs` (`TimeKey`, `Pid`, `Comments`) VALUES (now(),$$,'Read TagId $cur_tag');");

  if (($cur_tag ne '0'))  {

    $prv_tag=$cur_tag;
############### Set tare #########################
#   Read MOXA buffer
    $w_ok=1;
    while ($w_ok){
      undef $w;
      do {   
          $w = new IO::Socket::INET(
                   PeerAddr => '192.168.161.254',
                   PeerPort => 4001,
                   Proto => 'tcp', Timeout => 1);
      } while (!(defined $w));
    
      for (my $j=0;$j<64;$j++) {$w->recv($dg,1024);}
      $w->recv($dg,22);

      if ($dg=~m/(US|ST|OL),(GS|NT),.{2},.{11}/g) {
        @mmm = unpack ('A2A1A2A1H2b8A1A8A1A2', $dg);
        $mmm[7]=~s/\s+//;  #$mmm[9]=kg;
        if ($mmm[7]=~ m{
                        ^(
                        [-+]?    # отрицательное число тоже число.
                        [\d]+    # целое...
                        \.?[\d]* # ...или не целое, но всё равно число.
                        )$
                      }x) {$tare=$mmm[7];$w_ok=0;}
print "Tare\t$tare\n" if $d_bg;
if ($d_bg) {
 print "\nRecive: " . length($dg) . " byte\nRaw data:\t$dg\n";
 foreach my $m (@mmm) {print "Unpacked : \t $m\n" }
}
      }# read tare
    } # while
##################################################
    $w->close;
    ExecSql("INSERT INTO `Logs` (`TimeKey`, `Pid`, `Comments`) VALUES (now(),$$,'Tare $tare t');");
# 2.Set ToPlatform (M4)
    if (!($m->is_open()))
    {
      undef $m;
      do {   
        $m = MBclient->new();
        if ($m->is_open()) {$m->host("192.168.161.252"); $m->unit_id(1);}
      } while (!($m->is_open()));
    }
    $m->write_single_coil(2052, 1);
    
# 2.1 ReSet EndEnterWait (M5)
    if (!($m->is_open()))
    {
      undef $m;
      do {   
        $m = MBclient->new();
        if ($m->is_open()) {$m->host("192.168.161.252"); $m->unit_id(1);}
      } while (!($m->is_open()));
    }
    $m->write_single_coil(2053, 0);
print "Position...\n" if $d_bg;

    ExecSql("INSERT INTO `Logs` (`TimeKey`, `Pid`, `Comments`) VALUES (now(),$$,'Enable enter. Reset wait timer.');");

# 3.Read ErrPos,InPos,ToLarge,ToPlatform,EndEnterWait (M1,M2,M3,M4,M5)
    undef $bits;
    do {   
      if (!($m->is_open()))
      {
        undef $m;
        do {   
          $m = MBclient->new();
          if ($m->is_open()) {$m->host("192.168.161.252"); $m->unit_id(1);}
        } while (!($m->is_open()));
      }
      $bits = $m->read_coils(2049, 5);  
#print "ErrPos->InPos->ToLarge->$bits->$$bits[0]\t$$bits[1]\t$$bits[2]\n" if $d_bg;
    } while (!(($$bits[0]) or ($$bits[1]) or ($$bits[2]) or ($$bits[4])));;
    $m->close;
# 4.If InPos Read cas5010a. Write to DB
    if ($$bits[1] eq '1') {
      ExecSql("INSERT INTO `Logs` (`TimeKey`, `Pid`, `Comments`) VALUES (now(),$$,'Car in platform');");

      $mmm[7]='0.00';
      $prv_w='-1.00';

print "while ->($mmm[7] gt $prv_w)\tcur $mmm[7]\tprv $prv_w\n"if $d_bg;
      $w_ok=1;
      while ($w_ok)
      {
        do {   
            $w = new IO::Socket::INET(
                     PeerAddr => '192.168.161.254',
                     PeerPort => 4001,
                     Proto => 'tcp', Timeout => 1);
        } while (!(defined $w));

print "Read CAS...\n" if $d_bg;
print "mmm7\t$mmm[7]\n" if $d_bg;
        for (my $j=0;$j<64;$j++) { 
          $w->recv($dg,1024); 
#print "Read MOXA buffer "."."x$j."\n"  if $d_bg;
        }
        
        $w->recv($dg,22);
print "Recive: " . length($dg) . " byte\nRaw data:\t$dg\n" if $d_bg;

        if ($dg=~m/(US|ST|OL),(GS|NT),.{2},.{11}/g) {
          #US - unstable, ST - stable, OL - owerload
          #GS - gross (brutto), NT - netto
          #0 - CAS ID
          #1 Stable UpBorder LowBorder Unstable Netto Tare Zero
          # Weight - symbol
          @mmm = unpack ('A2A1A2A1H2b8A1A8A1A2', $dg);

          $mmm[7]=~s/\s+//;  #$mmm[9]=kg;
          if (!($mmm[7]=~ m{
                            ^(
                            [-+]?    # отрицательное число тоже число.
                            [\d]+    # целое...
                            \.?[\d]* # ...или не целое, но всё равно число.
                            )$
                          }x)) {
            $mmm[7]='0.00';
            $prv_w='-1.00';
print "Strange ->($mmm[7]\t$prv_w\n"if $d_bg;
          }
print " ->($mmm[7] gt $prv_w)\tcur $mmm[7]\tprv $prv_w\n"if $d_bg;
          if (($mmm[0] eq 'ST') and ($mmm[7] eq $prv_w))  {
# DB write weight
# 5.Set EndOfCycle (M0)
# 5.1.Read ID in Ch#1
            $ch1_tag='0';
            while ($cur_tag ne $ch1_tag){

              ReadTag(1,1,0x62); # Addr, channelNum, Cmd
              $p->recv($dg,42);
# check substr($dg,8,1)==0 (NO_ERR)
              $ch1_tag=unpack( 'N',reverse(substr($dg,10,ord(substr($dg,9,1)))));
              $p->close;
print "Ch1\tCurTag\t$cur_tag; PrvTag\t$prv_tag; Ch1_tag\t$ch1_tag\n" if $d_bg;
              if (($cur_tag ne $ch1_tag) and ($ch1_tag ne '0')){
                sleep(4); #time grate that 'memory' settings for channel and less that resend time
                $cur_tag=$ch1_tag;	
                $ch1_tag='0';
              } # $cur_tag ne $ch1_tag
            } # read ID in Ch#1
            ExecSql("INSERT INTO `Logs` (`TimeKey`, `Pid`, `Comments`) VALUES (now(),$$,'Weight $mmm[7] t');");
            ExecSql("INSERT INTO `Logs` (`TimeKey`, `Pid`, `Comments`) VALUES (now(),$$,'TagId $cur_tag in platform.');");

            @res = ExecSql("select TagId from tags where TagId=$cur_tag;");
#fetch row @{res[$line]}; fetch field $res[$line][$field]
            if ($res[0][0] eq $cur_tag) {
print ("INSERT INTO `reestr` (`TimeKey`, `Weight`, `Tare`, `TagId`, `Num`, `Comments`) VALUES (now(), $mmm[7], $tare, $cur_tag, (select Number from tags where TagId=$cur_tag), '');") if $d_bg;
              ExecSql("INSERT INTO `reestr` (`TimeKey`, `Weight`, `Tare`, `TagId`, `Num`, `Comments`) VALUES (now(), $mmm[7], $tare, $cur_tag, (select Number from tags where TagId=$cur_tag), '');");
            }
            else {
print ("Err Tag") if $d_bg;
              ExecSql("INSERT INTO `reestr` (`TimeKey`, `Weight`, `Tare`, `TagId`, `Num`, `Comments`) VALUES (now(), $mmm[7], $tare, $cur_tag, $cur_tag, 'Erorr read TagId');");
            }
            if (!($m->is_open()))
            {
              undef $m;
              do {   
                $m = MBclient->new();
                if ($m->is_open()) {$m->host("192.168.161.252"); $m->unit_id(1);}
              } while (!($m->is_open()));
            }
            $m->write_single_coil(2048, 1);
            $w_ok=0;
            @res=();
          } #ST 
          if ($mmm[0] eq 'OL') {
# DB write OwerLoad
# 5.Set EndOfCycle (M0)
print "Owerload.\n" if $d_bg;
            ExecSql("INSERT INTO `reestr` (`TimeKey`, `Weight`, `Tare`, `TagId`, `Num`, `Comments`) VALUES (now(), $mmm[7], $tare, $cur_tag, $cur_tag, 'Owerload');");
            if (!($m->is_open()))
            {
              undef $m;
              do {   
                $m = MBclient->new();
                if ($m->is_open()) {$m->host("192.168.161.252"); $m->unit_id(1);}
              } while (!($m->is_open()));
            }
            $m->write_single_coil(2048, 1);
          } #OL 
          $prv_w=$mmm[7];
        } # read CAS
      } # mmm ne '0.00'
      $w->close if defined $w;
      $m->close if $m->is_open();
    }  #InPos
    if ($$bits[0] eq '1') {
# DB write ErrPos
# 5.Set EndOfCycle (M0)
print "ErrPos.\n" if $d_bg;
      ExecSql("INSERT INTO `reestr` (`TimeKey`, `Weight`, `Tare`, `TagId`, `Num`, `Comments`) VALUES (now(), 0, 888, $cur_tag, $cur_tag, 'ErrorPos');");
      if (!($m->is_open()))
      {
        undef $m;
        do {   
          $m = MBclient->new();
          if ($m->is_open()) {$m->host("192.168.161.252"); $m->unit_id(1);}
        } while (!($m->is_open()));
      }
      $m->write_single_coil(2048, 1);
      $m->close if $m->is_open();
    }  #ErrPos
    if ($$bits[2] eq '1') {
# DB write ToLarge
# 5.Set EndOfCycle (M0)
print "Large!\n" if $d_bg;
      ExecSql("INSERT INTO `reestr` (`TimeKey`, `Weight`, `Tare`, `TagId`, `Num`, `Comments`) VALUES (now(), 0, 999, $cur_tag, $cur_tag, 'ToLarge');");
      if (!($m->is_open()))
      {
        undef $m;
        do {   
          $m = MBclient->new();
          if ($m->is_open()) {$m->host("192.168.161.252"); $m->unit_id(1);}
        } while (!($m->is_open()));
      }
      $m->write_single_coil(2048, 1);
      $m->close if $m->is_open();
    }  #ToLarge
    if ($$bits[4] eq '1') {
# EnterTimeOut
print "TimeOut\n" if $d_bg;
      ExecSql("INSERT INTO `reestr` (`TimeKey`, `Weight`, `Tare`, `TagId`, `Num`, `Comments`) VALUES (now(), 0, 555, $cur_tag, $cur_tag, 'ReadOutgoingTag');");
      if (!($m->is_open()))
      {
        undef $m;
        do {   
          $m = MBclient->new();
          if ($m->is_open()) {$m->host("192.168.161.252"); $m->unit_id(1);}
        } while (!($m->is_open()));
      }
      $m->write_single_coil(2053, 0);
      $m->close if $m->is_open();
    }  # EnterTimeOut
# 6.Read EndOfCycle (M0)
# 7.If (M0)==0 Goto 1
print "Go Away...\n" if $d_bg;
    undef $bits;
    do {
      if (!($m->is_open()))
      {
        undef $m;
        do {   
          $m = MBclient->new();
          if ($m->is_open()) {$m->host("192.168.161.252"); $m->unit_id(1);}
        } while (!($m->is_open()));
      }
      $bits = $m->read_coils(2048, 1);
print "$bits=>$$bits[0]\n" if $d_bg;
    } while (($$bits[0]) or !(defined $$bits[0]));
    $m->close if $m->is_open();
    ExecSql("INSERT INTO `Logs` (`TimeKey`, `Pid`, `Comments`) VALUES (now(),$$,'ErrPos=$$bits[0], InPos=$$bits[1], ToLarge=$$bits[2], ToPlatform=$$bits[3], EndEnterWait=$$bits[4]');");
  } #$cur_tag ne $prv_tag
  sleep(4);
} #while (1)


#### End ####
$m->close if $m->is_open();
$w->close if defined $w;
$p->close if defined $p;
ExecSql("INSERT INTO `Logs` (`TimeKey`, `Pid`, `Comments`) VALUES (now(),$$,'Exit...');");
$dbh->disconnect();

sub END_handler {
 $fail++;
 ExecSql("INSERT INTO `Logs` (`TimeKey`, `Pid`, `Comments`) VALUES (now(),$$,'Recive STOP signal');");
}


sub ReadTag
{          
  my @buf;
  binmode STDOUT;
#0x80 PACK_TYPE_EVENT    Пакет в формате внутреннего эвента
#0x81 PACK_TYPE_CONFIG   Пакет с данными конфигурации
#0x82 PACK_TYPE_COMMAND  Пакет содержит команду встроенному контроллеру
#0x83 PACK_TYPE_GET_DATA Пакет содержит статус контроллера
#0x84 PACK_TYPE_TRANSACT Пакет является транзакцией от считывателя

  $buf[0]=chr(0x01);  #$pkt_num;
  $buf[1]=chr($_[0]); #$addr;
  $buf[2]=chr(0x83);  #PACKET_TYPE_GET_DATA
  $buf[3]=chr(0x1A);  #NAME_TAG_STACK
  $buf[4]=chr($_[1]); #CHANNEL_1 / 2
  $buf[5]=chr(0x00);  # component
  $buf[6]=chr(0x00);  # subcomponent
  $buf[7]=chr($_[2]); # CMD_GET_FIRST_TAG
  $buf[8]=chr(0x00);  # data_lenght
  $buf[9]=chr(0x00);  # data 

#$crc==0 - packet OK
  my $i;
  my $index;
  my $Length = $#buf+1;
  my $crc = 0x5A;

  for ($i = 0; $i<$Length; $i++)
  {
   our @crc_table;
   $crc = $crc_table[($crc ^ ord($buf[$i]))];
  }
  $buf[$Length+1] = chr($crc);

my $pdata = join("",@buf);
  do {   
    $p = new IO::Socket::INET(
             LocalAddr => '192.168.161.229',
             LocalPort => 8873,
             PeerAddr => '192.168.161.253',
             PeerPort => 8872,
             Proto => 'udp', Timeout => 1);
  } while (!(defined $p));

  $p->send($pdata);
}

##########
sub DBConnect {

    $dbh = DBI->connect( "dbi:mysql:$_[1];host=$_[0]", $_[2], $_[3] );
    die "Can`t connect to DB :" . $DBI::errstr
      if ( !defined($dbh) );

#    $dbh->{AutoCommit} = 0;
#MySQL specific
    $dbh->{mysql_auto_reconnect}=1;
    $dbh->{mysql_enable_utf8}=1;

    $dbh->{LongReadLen} = 1000000;

    return $dbh;
};
#
sub ExecSql {
    my $sqltxt = $_[0];
    my ( $sth, $rv, @arr, $rc );
    if ( $sqltxt =~ /^\s*select\s/i ) {
        $sth = $dbh->prepare($sqltxt) || die "Error parcing sql ";
        $rv = $sth->execute || die "Execution error $sqltxt\n$DBI::errstr" ;
        while (my @str=$sth->fetchrow_array()) {
          push(@arr, [@str]);
        }
        $sth->finish;
        return @arr;
    } else {
        $rc = $dbh->do($sqltxt) ||
          die "Execution error $sqltxt";
        return $rc;
    }
};


########## reestr
#Column         Type            Comment
#TimeKey        datetime                Время
#Weight         float unsigned [0]      Вес с тарой
#Tare           float                   Тара
#TagId          int(11) unsigned        RF метка
#Num            int(11)                 Номер гаражный
#Comments       text                    Примечания
########## tags
#TagID  int(11) unsigned        Десятичный номер метки
#Number int(11) unsigned        Гаражный номер
