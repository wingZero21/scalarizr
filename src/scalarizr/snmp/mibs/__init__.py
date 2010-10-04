
def validate(syntax, value, clone = True):
	value = int(value)
	spec = syntax.subtypeSpec
	rangeconstr = spec.getValueMap().keys()[0] 
	stop = rangeconstr.stop
	
	if stop < value:
		return syntax.clone(value % stop) if clone else value % stop
	return syntax.clone(value) if clone else value