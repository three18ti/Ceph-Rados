package Ceph::RADOS;
# ABSTRACT: Ceph::RADOS rados bindings for perl
use strict;
use Exporter;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);
@ISA = qw(Exporter);
$VERSION = '0.002';
use Inline C        => 'DATA',
           VERSION  => '0.002',
           NAME     => 'Ceph::RADOS',
	   LIBS	    => '-L/usr/lib -lrados',
           INC      => '-I/usr/include/rados',
	   TYPEMAPS => 'lib/Ceph/types',
		BUILD_NOISY => 1,
		WARNINGS => 1,
		PRINT_INFO => 1,
;

sub new {
  my $self = {};
  $self->{conn} = create_c();
  if($self->{conn} ne undef){
    bless $self;
    return $self;
  }
  print "Unable to create new RADOS object\n";
  return 0;
}

sub connect {
  my $self = shift;
  my $settings = shift;
  
  foreach my $opt(keys %{$settings}){
    print "Unable to set config '$opt' = $settings->{$opt}\n" if conf_set_c($self->{conn},$opt,$settings->{$opt}) < 0;
  }
  
  if(connect_c($self->{conn}) < 0){
    print "Unable to initiate connection\n";
    return 0;
  }  

  return 1;
}

sub disconnect {
  my $self = shift;
  disconnect_c($self->{conn});
}

sub cluster_stat {
 my $self = shift;
 my @stats = cluster_stat_c($self->{conn});
 return \@stats;
}

sub open_pool {
  my $self = shift;
  my $poolname = shift;
  return 1 if open_pool_c($self->{conn},$poolname) == 0;
  return 0;
}

sub close_pool {
  my $self = shift;
  my $poolname = shift;
  close_pool_c();
}

sub create_pool {
  my $self = shift;
  my $poolname = shift;
  create_pool_c($self->{conn},$poolname);
}

sub delete_pool {
  my $self = shift;
  my $poolname = shift;
  delete_pool_c($self->{conn},$poolname);
}

sub list_pools {
  my $self = shift;
  my @pools = list_pools_c($self->{conn});
  return \@pools;
}

sub lookup_pool{
  my $self = shift;
  my $poolname = shift;
  my $num = lookup_pool_c($self->{conn},$poolname);
  return undef if $num == -2;
  return $num;
}

sub get_pool_id{
  my $self = shift;
  my $id = get_pool_id_c();
  return $id;  
}

sub get_pool_auid{
  my $self = shift;
  my $id = get_pool_auid_c();
  return $id;
}

sub set_pool_auid{
  my $self = shift;
  my $auid = shift;
  return 1 if set_pool_auid_c($auid) == 0;
  return 0;
}

1;

__DATA__

=pod

=cut

__C__
#include <stdio.h>
#include <stdlib.h>
#include <librados.h>
#include <errno.h>

rados_ioctx_t io_ctx;

void rados_err (char * desc, int err) {
  err = abs(err);
  printf("error(%d) in %s:%s\n", err, desc, strerror(err));
}

rados_t create_c () {
  rados_t clu;
  int ret = rados_create(&clu, NULL);
  
  if(ret == 0)
    return clu;

  rados_err("create",ret);
  return NULL;
}

int conf_set_c (rados_t clu, char * opt, char * val) {
  int ret = rados_conf_set(clu,opt,val);
  
  if(ret == 0)
    return ret;

  rados_err("conf_set",ret);
  return -1;
}

int connect_c (rados_t clu) {

  int ret = rados_connect(clu);
  
  if(ret == 0){
    return ret;
  }
  
  rados_err("connect",ret);
  return -1;
}

void disconnect_c (rados_t clu) {
  rados_shutdown(clu);
}

int create_pool_c (rados_t clu, char * poolname) {
  int ret = rados_pool_create(clu, poolname);

  if(ret == 0)
    return 0;

  rados_err("create_pool",ret);
  return ret;

}

int delete_pool_c (rados_t clu, char * poolname) {
  int ret = rados_pool_delete(clu,poolname);

  if(ret == 0)
    return 0;

  rados_err("delete_pool",ret);
  return ret;
}

void list_pools_c (rados_t clu) {
  Inline_Stack_Vars;

  size_t buf_sz = rados_pool_list(clu,"",0);
  // fprintf(stderr, "rados_pool_list()=%d\n", buf_sz);

  char buf[buf_sz];
  size_t r = rados_pool_list(clu,buf,buf_sz);

  if (r != buf_sz) {
    printf("buffer size mismatch: got %d the first time, but %d "
    "the second.\n", buf_sz, r);
  }

  Inline_Stack_Reset;

  const char *b = buf;
  while(1) {
    if(b[0] == '\0') {
      Inline_Stack_Done;
      break;
    }
    Inline_Stack_Push(sv_2mortal(newSVpv(b,0)));
    b += strlen(b) +1;
  }

}

void cluster_stat_c (rados_t clu){
  struct rados_cluster_stat_t result;
  int ret = rados_cluster_stat(clu,&result);
  
  if(ret != 0)
    rados_err("cluster_stat",ret);
  else {
    Inline_Stack_Vars;
    Inline_Stack_Reset;
    Inline_Stack_Push(sv_2mortal(newSViv(result.kb)));
    Inline_Stack_Push(sv_2mortal(newSViv(result.kb_used)));
    Inline_Stack_Push(sv_2mortal(newSViv(result.kb_avail)));
    Inline_Stack_Push(sv_2mortal(newSViv(result.num_objects)));
    Inline_Stack_Done;
  }
  
}

int open_pool_c(rados_t clu,char * poolname) {
  int ret = rados_ioctx_create(clu,poolname,&io_ctx);
  
  if(ret == 0)
    return 0;

  rados_err("open_pool",ret);
  return ret;
}

void close_pool_c() {
  rados_ioctx_destroy(io_ctx);
}


int lookup_pool_c (rados_t clu,const char * poolname) {
  int ret = rados_pool_lookup(clu,poolname);
  
  return ret;
}

int get_pool_id_c (){
  int ret = rados_ioctx_get_id(io_ctx);

  if(ret >= 0)
    return ret;

  rados_err("get_pool_id_c",ret);
  return ret;
}

int get_pool_auid_c (){
  uint64_t auid;
  int ret = rados_ioctx_pool_get_auid(io_ctx,&auid);

  if(ret == 0)
    return auid;

  rados_err("get_pool_auid_c",ret);
  return ret;
}

int set_pool_auid_c(uint64_t auid){
  int ret = rados_ioctx_pool_set_auid(io_ctx,auid);

  if(ret == 0)
    return ret;

  rados_err("set_pool_auid",ret);
  return ret;
}
