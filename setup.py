from setuptools import setup, findall, find_packages

description = "Scalarizr converts any server to Scalr-manageable node"

cfg = dict(
	name = "scalarizr",
	version = "0.5",	 
	description = description,
	long_description = description,
	author = "Scalr Inc.",
	author_email = "info@scalr.net",
	url = "https://scalr.net",
	license = "GPL",
	platforms = "any",
	package_dir = {"" : "src"},
	packages = find_packages("src"),
	requires = ["m2crypto (>=0.20)", "boto"],
	data_files= [
		("/etc/scalr", findall("etc")),
		("/usr/local/scalarizr/scripts", findall("scripts"))
	]
)



setup(**cfg)


