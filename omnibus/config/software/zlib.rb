name "zlib"
default_version "1.2.6"

version "1.2.6" do
  source md5: "618e944d7c7cd6521551e30b32322f4a"
end

version "1.2.8" do
  source md5: "44d667c142d7cda120332623eab69f40"
end

source url: "http://downloads.sourceforge.net/project/libpng/zlib/#{version}/zlib-#{version}.tar.gz"

relative_path "zlib-#{version}"

build do
  # We omit the omnibus path here because it breaks mac_os_x builds by picking
  # up the embedded libtool instead of the system libtool which the zlib
  # configure script cannot handle.
  env = with_standard_compiler_flags
  if windows?
    if system("echo $0") # we're on mingw or cygwin
        configure = "sh .\\configure"
    elsif system('$PSVersionTable.PSVersion')
        configure = "configure"
    end
  else
    configure = "./configure"
  end
  # For some reason zlib needs this flag on solaris (cargocult warning?)
  env['CFLAGS'] << " -DNO_VIZ" if solaris2?

  command "#{configure}" \
          " --prefix=#{install_dir}/embedded", env: env

  make "-j #{max_build_jobs}", env: env
  make "-j #{max_build_jobs} install", env: env
end