require 'formula'

class MysqlReplicationListener < Formula
  url 'https://github.com/SponsorPay/mysql-replication-listener.git'
  homepage 'https://github.com/SponsorPay/mysql-replication-listener'

  depends_on 'cmake'
  depends_on 'boost'
  #depends_on 'openssl'

  def install
    system 'cmake', "-DCMAKE_INSTALL_PREFIX:PATH=#{prefix}", '.'
    system 'make'
    system 'make install'
  end
end
