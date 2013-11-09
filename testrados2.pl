#!/usr/bin/perl
use strict;
use Ceph::RADOS;

my $c = new Ceph::RADOS;
my $testpool = 'this_test_pool';
my $mon_host = shift || '127.0.0.1';

# test connect
if($c->connect({'mon_host' => $mon_host})) {
  print "connected\n";

  my $stat = $c->cluster_stat;
  print "Kbytes: $stat->[0]\n";
  print "Kbytes used: $stat->[1]\n";
  print "Kbytes avail: $stat->[2]\n";
  print "Objects: $stat->[3]\n";

  # delete pool if existing
  $c->delete_pool($testpool) if $c->lookup_pool($testpool);

  #test create pool
  $c->create_pool($testpool);
  if($c->lookup_pool($testpool) ne undef) {
    print "create pool $testpool success\n";
  }
  else {
    print "create pool $testpool fail\n";
  }

  #test list pool
  my $list = $c->list_pools();
  foreach(@{$list}) {
    my $i = $c->lookup_pool($_);
    print "pool:($i)  $_\n";

  }

  #test open pool
  if($c->open_pool($testpool)){
    print "open pool success\n";
    my $id = $c->get_pool_id();
    print "pool id: $id\n";
    my $auid = $c->get_pool_auid();
    print "pool auid: $auid\n";
    $auid = $c->get_pool_auid() if $c->set_pool_auid(232323);
    print "new pool auid: $auid\n";
  }
  else{
    print "open pool fail\n";
  }

  # close pool
  $c->close_pool($testpool);

  #test delete pool
  if($c->lookup_pool($testpool) ne undef){
    $c->delete_pool($testpool);
    if($c->lookup_pool($testpool) ne undef) {
      print "delete pool $testpool fail\n";
    }
    else {
      print "delete pool $testpool success\n";
    }
  }

  $c->disconnect();
}
else{
  print "failed to connect\n";
}
