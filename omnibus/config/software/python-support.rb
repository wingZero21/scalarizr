name "python-support"
version "1.0.15"

dependency "python"

source :url => "https://launchpad.net/ubuntu/+archive/primary/+files/python-support_#{version}.tar.gz",
       :md5 => "fa98675199468943db49deab6f147ebd"

relative_path "python-support-#{version}"

configure_env = {
  "LDFLAGS" => "-L#{install_dir}/embedded/lib -I#{install_dir}/embedded/include -L/lib -L/usr/lib",
  "CFLAGS" => "-L#{install_dir}/embedded/lib -I#{install_dir}/embedded/include",
  "LD_RUN_PATH" => "#{install_dir}/embedded/lib",
  "PATH" => "#{install_dir}/embedded/bin:#{ENV["PATH"]}"
}

build do
  command "./configure --prefix=#{install_dir}/embedded", :env => configure_env
  command "make -j #{max_build_jobs}", :env => {"LD_RUN_PATH" => "#{install_dir}/embedded/lib"}
  command "make install"
end