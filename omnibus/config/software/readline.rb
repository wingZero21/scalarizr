name "readline"
default_version "5.2"

source :url => "ftp://ftp.gnu.org/gnu/readline/readline-#{default_version}.tar.gz",
       :md5 => "e39331f32ad14009b9ff49cc10c5e751"

relative_path "readline-#{default_version}"

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
