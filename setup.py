
cfg = dict(
	name="scalarizr",
	version="0.5",	 
	description="Scalarizr converts any server to Scalr-manageable node",
	author="Scalr Inc.",
	package_dir={"" : "src"},
	packages=[
		"scalarizr", 
		"scalarizr.handlers", 
		"scalarizr.handlers.ec2",
		"scalarizr.messaging",
		"scalarizr.messaging.p2p",
		"scalarizr.platform",
		"scalarizr.scripts",
		"scalarizr.util" 
	],
	scripts=['bin/scalarizr']
)


from distutils.core import setup
setup(**cfg)


