require 'formula'

class MysqlReplicationListener < Formula
  url 'https://bitbucket.org/winebarrel/ruby-binlog/downloads/mysql-replication-listener.tar.gz'
  homepage 'https://launchpad.net/mysql-replication-listener'
  md5 'c91735dd044aa5ef9e62991cb9a93d28'

  depends_on 'cmake'
  depends_on 'boost'
  #depends_on 'openssl'

  def install
    system 'cmake', '.'
    system 'make'
    system 'make install'
  end

  def patches
    {:p0 => %w(
        mysql-replication-listener-as_c_str.patch
        mysql-replication-listener-longlong.patch
        mysql-replication-listener-enum.patch
        mysql-replication-listener-date.patch
        mysql-replication-listener-time-year.patch
        mysql-replication-listener-decimal.patch
      ).map {|i| "https://bitbucket.org/winebarrel/ruby-binlog/downloads/#{i}" }
    }
  end
end
