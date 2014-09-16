name "gdbm"
default_version "1.9.1"

dependency "libgcc"

source url: "http://ftp.gnu.org/gnu/gdbm/gdbm-1.9.1.tar.gz",
       md5: "59f6e4c4193cb875964ffbe8aa384b58"

relative_path "gdbm-1.9.1"

build do
  env = with_standard_compiler_flags(with_embedded_path)
  if windows?
    if system("echo $0") # we're on mingw or cygwin
        configure = "sh .\\configure"
    elsif system('$PSVersionTable.PSVersion')
        configure = "configure"
    end
  else
    configure = "./configure"
  end

  if freebsd?
    command "./configure" \
            " --enable-libgdbm-compat" \
            " --with-pic" \
            " --prefix=#{install_dir}/embedded", env: env
  else
    command "#{configure}" \
            " --enable-libgdbm-compat" \
            " --prefix=#{install_dir}/embedded", env: env
  end

  make "-j #{max_build_jobs}", env: env
  make "install", env: env
end