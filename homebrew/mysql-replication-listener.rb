require 'formula'

class MysqlReplicationListener < Formula
  url 'https://bitbucket.org/winebarrel/mysql-replication-listener.git'
  homepage 'https://bitbucket.org/winebarrel/mysql-replication-listener'

  depends_on 'cmake'
  depends_on 'boost'
  #depends_on 'openssl'

  def install
    system 'cmake', '.'
    system 'make'
    system 'make install'
  end
end
