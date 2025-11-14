#!/usr/bin/env node

/**
 * Debug script to monitor db-writer worker status
 * Run this to check if the worker is hanging and get debug information
 */

const { dbWriterWorker } = require('./dist/workers/db-writer');

function checkWorkerStatus() {
  console.log('=== DB-Writer Worker Debug Status ===');
  console.log(new Date().toISOString());

  try {
    const status = dbWriterWorker.getDebugStatus();

    console.log('\nCurrent Operation:', status.currentOperation);
    console.log('Operation Started:', status.operationStartTime);
    console.log('Last Progress:', status.lastProgressTimestamp);
    console.log('Time Since Last Progress:', `${status.timeSinceLastProgress}ms`);

    if (status.timeSinceLastProgress > 60000) {
      console.log('⚠️  WARNING: Worker has not made progress for over 1 minute!');
    }

    if (status.timeSinceLastProgress > 300000) {
      console.log('🚨 CRITICAL: Worker has not made progress for over 5 minutes!');
    }

    console.log('\nDebug Configuration:');
    console.log('  SQL Queries:', status.debugConfig.sqlQueries);
    console.log('  Query Timeout:', status.debugConfig.queryTimeout);
    console.log('  Detailed Timing:', status.debugConfig.detailedTiming);

    console.log('\n=== End Debug Status ===');

  } catch (error) {
    console.error('Error getting debug status:', error);
  }
}

// If run directly, check status once
if (require.main === module) {
  checkWorkerStatus();

  // Optional: monitor continuously
  if (process.argv.includes('--watch')) {
    console.log('\n🔍 Monitoring db-writer worker (press Ctrl+C to stop)...');
    setInterval(checkWorkerStatus, 5000);
  }
}

module.exports = { checkWorkerStatus };