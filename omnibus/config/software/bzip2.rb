name "bzip2"
version "1.0.6"

source :url => "http://www.bzip.org/#{version}/bzip2-#{version}.tar.gz",
       :md5 => "00b516f4704d4a7cb50a1d97e6e8e15b"

relative_path "bzip2-#{version}"

env = {
  "LDFLAGS" => "-L#{install_dir}/embedded/lib -I#{install_dir}/embedded/include",
  "CFLAGS" => "-L#{install_dir}/embedded/lib -I#{install_dir}/embedded/include",
  "LD_RUN_PATH" => "#{install_dir}/embedded/lib"
}

build do
  command "make -f Makefile-libbz2_so", :env => env
  command "cp -lf libbz2.so.* #{install_dir}/embedded/lib"
  command "ln -sf #{install_dir}/embedded/lib/libbz2.so.#{version} #{install_dir}/embedded/lib/libbz2.so.1"
end
