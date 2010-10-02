'''
Created on Sep 27, 2010

@author: marat
'''
from scalarizr.bus import bus
from scalarizr.queryenv import ScalingMetric
from scalarizr.util import read_shebang

import os, time
from subprocess import Popen, PIPE

# SNMP imports
from pysnmp.smi.builder import MibBuilder
(MibScalarInstance, MibTableRow, MibTable, MibTableColumn, Integer32) = mibBuilder.importSymbols(
		'SNMPv2-SMI',
		'MibScalarInstance', 'MibTableRow' , 'MibTable', 'MibTableColumn', 'Integer32')
(mtxTable, mtxEntry, mtxIndex, mtxId, mtxName, mtxValue, mtxError) = mibBuilder.importSymbols(
		'SCALING-METRICS-MIB', 
		'mtxTable', 'mtxEntry', 'mtxIndex', 'mtxId', 'mtxName', 'mtxValue', 'mtxError')


class MtxTableImpl(MibTable):

	EXEC_TIMEOUT = 5
	'''
	Executing timeout for script when obtain metric with 'execute' method 
	'''

	_metrics = None
	_metrics_timestamp = None
	
	def getNextNode(self, name, idx):
		mibBuilder.lastBuildId += 1
		
		# Clean old values
		for k in mibBuilder.mibSymbols['__SCALING-METRICS-MIB'].keys():
			if k.startswith('mtxIndex') or k.startswith('mtxId') or k.startswith('mtxName') or k.startswith('mtxValue'):
				del mibBuilder.mibSymbols['__SCALING-METRICS-MIB'][k]
				
		# Update with new values
		mibBuilder.mibSymbols['__SCALING-METRICS-MIB'].update(self.values())
		return MibTable.getNextNode(self, name, idx)

	def values(self):
		queryenv = bus.queryenv_service
		
		# Obtain scaling metrics from Scalr. Cache result for 30 minutes
		now = time.time()
		if not self._metrics or now - self._metrics_timestamp > 1800:
			self._metrics = queryenv.get_scaling_metrics()
			self._metrics_timestamp = now

		# TODO: investigate how efficiently will be do calculations in parallel
		
		ret = dict()
		index = 0
		for metric in self._metrics:
			index += 1
			
			error = ''
			value = 0.0
			try:
				# Retrieve metric value
				if ScalingMetric.RetriveMethod.EXECUTE == metric.retrieve_method:
					value = self._get_execute(metric)
				elif ScalingMetric.RetriveMethod.READ == metric.retrieve_method:
					value = self._get_read(metric)
				else:
					raise BaseException('Unknown retrieve method %s' % metric.retrieve_method)
				
				# Convert value to float
				try:
					value = float(value)
				except ValueError, e:
					raise ValueError("Cannot convert value '%s' to float" % value)
					
			except (BaseException, Exception), e:
				value = 0.0
				error = str(e)[0:255]
		
				
			# Export MibTableRow
			ret.update({
				'mtxIndex%s' % index : MibScalarInstance(mtxIndex.getName(), (index,), mtxIndex.getSyntax().clone(
					index
				)),
				'mtxId%s' % index : MibScalarInstance(mtxId.getName(), (index,), mtxId.getSyntax().clone(
					int(metric.id)
				)),
				'mtxName%s' % index : MibScalarInstance(mtxName.getName(), (index,), mtxName.getSyntax().clone(
					metric.name
				)),
				'mtxValue%s' % index : MibScalarInstance(mtxValue.getName(), (index,), mtxValue.getSyntax().clone(
					'%.7f' % value
				)),
				'mtxError%s' % index : MibScalarInstance(mtxError.getName(), (index,), mtxError.getSyntax().clone(
					error
				))
			})
		
		return ret
	
	def _get_execute(self, metric):
		if not os.access(metric.path, os.X_OK):
			raise BaseException("File is not executable: '%s'" % metric.path)
		
		proc = Popen(metric.path, stdout=PIPE, stderr=PIPE)
		start_time = time.time()
		while time.time() - start_time < self.EXEC_TIMEOUT:
			if proc.poll() is None:
				time.sleep(0.5)
			else:
				break
		else:
			if hasattr(proc, 'kill'):
				# python >= 2.6
				proc.kill()
			else:
				import signal
				os.kill(proc.pid, signal.SIGKILL)
			raise BaseException('Timeouted')						

		stdout, stderr = proc.communicate()
		if proc.returncode > 0:
			raise BaseException(stderr if stderr else 'exitcode: %d' % proc.returncode)
		
		return stdout
	
	def _get_read(self, metric):
		if not os.access(metric.path, os.R_OK):
			raise BaseException("File is not readable: '%s'" % metric.path)
		
		file = None
		try:
			file = open(metric.path, 'r')
			value = file.readline()
		finally:
			if file:
				file.close()
			del file
		
		return value
	
# SNMP exports
mtxTable = MtxTableImpl(mtxTable.getName())
exports = dict(mtxTable=mtxTable)
exports.update(mtxTable.values())
mibBuilder.exportSymbols('__SCALING-METRICS-MIB', **exports)
