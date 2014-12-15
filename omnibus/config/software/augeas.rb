name "augeas"
default_version "1.2.0"

dependency "libxml2"
dependency "readline"

source :url => "http://download.augeas.net/augeas-#{version}.tar.gz",
       :md5 => "dce2f52cbd20f72c7da48e014ad48076"

relative_path "augeas-#{version}"
env = with_standard_compiler_flags(with_embedded_path)

if rhel?
  if _64_bit?
    libdir = '/lib64'
  else
    libdir = '/lib'
  end

  whitelist_file "#{libdir}/libselinux.so.1"
  whitelist_file "#{libdir}/libsepol.so.1"
end

build do
    block do
        env["LIBXML_CFLAGS"] = `#{install_dir}/embedded/bin/xml2-config --cflags`.strip
        env["LIBXML_LIBS"] = `#{install_dir}/embedded/bin/xml2-config --libs`.strip
    end
    command "./configure --prefix=#{install_dir}/embedded", :env => env
    command "make -j #{max_build_jobs}", :env => env
    command "make install"
end
