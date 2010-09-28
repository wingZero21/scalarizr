'''
Created on Sep 27, 2010

@author: marat
'''
from scalarizr.bus import bus
from scalarizr.queryenv import ScalingMetric
from scalarizr.util import read_shebang

import os
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
	def getNextNode(self, name, idx):
		mibBuilder.lastBuildId += 1
		
		# Clean old values
		for k in mibBuilder.mibSymbols['__SCALING-METRICS-MIB'].keys():
			if k.startswith('mtxIndex') or k.startswith('mtxId') or k.startswith('mtxName') or k.startswith('mtxValue'):
				del mibBuilder.mibSymbols['__SCALING-METRICS-MIB'][k]
				
		# Update with new values
		mibBuilder.mibSymbols['__SCALING-METRICS-MIB'].update(MtxTableImpl.values())
		return MibTable.getNextNode(self, name, idx)

	@staticmethod
	def values():
		queryenv = bus.queryenv_service
		# TODO: cache response for 30 minutes		
		metrics = queryenv.get_scaling_metrics()
		
		# TODO: investigate how efficiently will be do calculations in parallel
		
		ret = dict()
		index = 0
		for metric in metrics:
			index += 1
			
			error = ''
			value = 0.0
			try:
				# Retrieve metric value
				if ScalingMetric.RetriveMethod.EXECUTE == metric.retrieve_method:
					if not os.access(metric.path, os.X_OK):
						raise BaseException("File is not executable: '%s'" % metric.path)
					
					proc = Popen(metric.path, stdout=PIPE, stderr=PIPE)
					
					stdout, stderr = proc.communicate()
					if proc.returncode > 0:
						raise BaseException(stderr if stderr else 'exitcode: %d' % proc.returncode)
					
					value = stdout
					del stdout, stderr, proc
				
				elif ScalingMetric.RetriveMethod.READ == metric.retrieve_method:
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
	
# SNMP exports
exports = dict(mtxTable=MtxTableImpl(mtxTable.getName()))
exports.update(MtxTableImpl.values())
mibBuilder.exportSymbols('__SCALING-METRICS-MIB', **exports)
