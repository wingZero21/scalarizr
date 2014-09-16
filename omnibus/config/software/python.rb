name "python"
if windows?

  default_version "2.7.8"

  source :url => "http://www.python.org/ftp/python/#{version}/python-#{version}.amd64.msi",
         :md5 => '38cadfcac6dd56ecf772f2f3f14ee846'

  relative_path "Python-#{version}"

  build do
    block do
      FileUtils.mkdir_p(File.expand_path("embedded/python", install_dir))
    end
    target_dir = File.join(install_dir, "embedded", "python").gsub!('/', '\\')
    command "msiexec /a python-#{version}.amd64.msi /qb TARGETDIR=#{target_dir}"
  end

else
  default_version "2.7.5"

  dependency "gdbm"
  dependency "ncurses"
  dependency "zlib"
  dependency "openssl"
  dependency "bzip2"

  source url: "http://python.org/ftp/python/#{version}/Python-#{version}.tgz",
         md5: 'b4f01a1d0ba0b46b05c73b2ac909b1df'

  relative_path "Python-#{version}"

  build do
    env = {
      "CFLAGS" => "-I#{install_dir}/embedded/include -O3 -g -pipe",
      "LDFLAGS" => "-Wl,-rpath,#{install_dir}/embedded/lib -L#{install_dir}/embedded/lib"
    }

    command "./configure" \
            " --prefix=#{install_dir}/embedded" \
            " --enable-shared" \
            " --with-dbmliborder=gdbm", env: env

    make env: env
    make "install", env: env

    # There exists no configure flag to tell Python to not compile readline
    delete "#{install_dir}/embedded/lib/python2.7/lib-dynload/readline.*"

    # Remove unused extension which is known to make healthchecks fail on CentOS 6
    delete "#{install_dir}/embedded/lib/python2.7/lib-dynload/_bsddb.*"
  end
end