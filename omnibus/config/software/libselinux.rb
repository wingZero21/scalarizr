name name "libselinux"
default_version "2.2.2"

source :url => "http://libselinux.sourcearchive.com/downloads/2.2.2-1/libselinux_2.2.2.orig.tar.gz",
       :md5 => "55026eb4654c4f732a27c191b39bebaf"

relative_path "libselinux-#{default_version}"

env = with_standard_compiler_flags(with_embedded_path)

build do
  command "make -j #{max_build_jobs}", :env => env
  command "make install"
end
