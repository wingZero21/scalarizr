name "sqlite3"
version "3080401"

dependency "readline"

source :url => "http://www.sqlite.org/2014/sqlite-autoconf-#{version}.tar.gz"
       :md5 => "6b8cb7b9063a1d97f7b5dc517e8ee0c4"

relative_path "sqlite-autoconf-#{version}"

env = {
  "LDFLAGS" => "-L#{install_dir}/embedded/lib -I#{install_dir}/embedded/include",
  "CFLAGS" => "-L#{install_dir}/embedded/lib -I#{install_dir}/embedded/include",
  "LD_RUN_PATH" => "#{install_dir}/embedded/lib"
}

build do
  command "./configure --prefix=#{install_dir}/embedded", :env => env
  command "make -j #{max_build_jobs}", :env => env
  command "make install"
end
