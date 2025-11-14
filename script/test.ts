// scripts/test-market-ws.ts
import WebSocket from 'ws';
import dotenv from 'dotenv'; 

dotenv.config();

// Configuration logging
console.log('🚀 Starting Polymarket WebSocket Test');
console.log('='.repeat(50));

const url = process.env.POLYMARKET_WS_URL ?? 'wss://ws-subscriptions-clob.polymarket.com';
const path = process.env.POLYMARKET_WS_PATH ?? '/ws/market';
const assets = (process.env.POLYMARKET_WS_ASSET_IDS ?? '')
  .split(',')
  .map((id) => id.trim())
  .filter(Boolean);

const auth = {
  apiKey: process.env.POLYMARKET_API_KEY!,
  secret: process.env.POLYMARKET_SECRET!,
  passphrase: process.env.POLYMARKET_PASSPHRASE!
};

// Log configuration (without sensitive data)
console.log('📋 Configuration:');
console.log(`   WebSocket URL: ${url}${path}`);
console.log(`   Asset IDs: [${assets.join(', ')}] (${assets.length} markets)`);
console.log(`   API Key: ${auth.apiKey.substring(0, 8)}...`);
console.log(`   Secret: ${auth.secret.substring(0, 8)}...`);
console.log(`   Passphrase: ${auth.passphrase.substring(0, 8)}...`);

// Connection state tracking
let messageCount = 0;
let pingCount = 0;
let lastMessageTime: Date | null = null;
let connectionStartTime = new Date();
const messageTypes = new Map<string, number>();

const ws = new WebSocket(`${url}${path}`);

// Enhanced connection logging
ws.on('open', () => {
  const connectTime = new Date();
  const connectionDuration = connectTime.getTime() - connectionStartTime.getTime();
  
  console.log('\n✅ WebSocket Connected');
  console.log(`   Connected in: ${connectionDuration}ms`);
  console.log(`   Ready State: ${ws.readyState}`);
  console.log(`   URL: ${ws.url}`);
  
  const subscription = {
    type: 'market',
    assets_ids: assets,
    auth: auth
  };
  
  console.log('\n📤 Sending subscription request:');
  console.log(`   Type: ${subscription.type}`);
  console.log(`   Assets: [${subscription.assets_ids.join(', ')}]`);
  
  try {
    ws.send(JSON.stringify(subscription));
    console.log('✅ Subscription sent successfully');
  } catch (error) {
    console.error('❌ Failed to send subscription:', error);
  }
});

ws.on('message', (raw) => {
  const now = new Date();
  const text = raw.toString();
  messageCount++;
  lastMessageTime = now;
  
  // Handle PING/PONG
  if (text === 'PING') {
    pingCount++;
    console.log(`🏓 PING received #${pingCount} at ${now.toISOString()}`);
    
    try {
      ws.send('PONG');
      console.log('🏓 PONG sent');
    } catch (error) {
      console.error('❌ Failed to send PONG:', error);
    }
    return;
  }
  
  // Parse and log JSON messages
  try {
    const data = JSON.parse(text);
    
    // Determine message type for statistics
    let messageType = 'unknown';
    if (data.type) {
      messageType = data.type;
    } else if (data.event) {
      messageType = data.event;
    } else if (Array.isArray(data)) {
      messageType = 'array';
    } else if (typeof data === 'object' && Object.keys(data).length > 0) {
      messageType = 'object';
    }
    
    messageTypes.set(messageType, (messageTypes.get(messageType) || 0) + 1);
    
    console.log(`\n📨 Message #${messageCount} (${messageType}) at ${now.toISOString()}`);
    console.log('─'.repeat(60));
    console.log(JSON.stringify(data, null, 2));
    console.log('─'.repeat(60));
    
  } catch (parseError) {
    console.log(`\n📨 Raw Message #${messageCount} at ${now.toISOString()}`);
    console.log(`⚠️  JSON Parse Error: ${parseError instanceof Error ? parseError.message : String(parseError)}`);
    console.log('─'.repeat(60));
    console.log(text);
    console.log('─'.repeat(60));
  }
});

// Enhanced error handling
ws.on('error', (error) => {
  console.error('\n❌ WebSocket Error:');
  console.error(`   Error: ${error.message}`);
  console.error(`   Type: ${error.name}`);
  console.error(`   Stack: ${error.stack}`);
});

ws.on('close', (code, reason) => {
  const closeTime = new Date();
  const sessionDuration = closeTime.getTime() - connectionStartTime.getTime();
  
  console.log('\n🔌 WebSocket Connection Closed');
  console.log(`   Close Code: ${code}`);
  console.log(`   Close Reason: ${reason || 'No reason provided'}`);
  console.log(`   Session Duration: ${sessionDuration}ms`);
  console.log(`   Total Messages: ${messageCount}`);
  console.log(`   Total PINGs: ${pingCount}`);
  
  // Show message type statistics
  if (messageTypes.size > 0) {
    console.log('\n📊 Message Type Breakdown:');
    for (const [type, count] of messageTypes.entries()) {
      console.log(`   ${type}: ${count}`);
    }
  }
  
  console.log('\n👋 Test completed');
});

// Add periodic status reporting
setInterval(() => {
  if (ws.readyState === WebSocket.OPEN) {
    const now = new Date();
    const timeSinceLastMessage = lastMessageTime 
      ? now.getTime() - lastMessageTime.getTime() 
      : now.getTime() - connectionStartTime.getTime();
    
    console.log(`\n📈 Status Report at ${now.toISOString()}:`);
    console.log(`   Connection State: OPEN`);
    console.log(`   Messages Received: ${messageCount}`);
    console.log(`   PINGs Handled: ${pingCount}`);
    console.log(`   Time Since Last Message: ${timeSinceLastMessage}ms`);
    console.log(`   Active Message Types: ${messageTypes.size}`);
  } else {
    const readyStateNames = ['CONNECTING', 'OPEN', 'CLOSING', 'CLOSED'];
    console.log(`\n📈 Status Report: Connection State: ${ws.readyState} (${readyStateNames[ws.readyState] || 'UNKNOWN'})`);
  }
}, 30000); // Every 30 seconds

// Handle process termination
process.on('SIGINT', () => {
  console.log('\n\n🛑 SIGINT received - closing connection...');
  ws.close();
});

process.on('SIGTERM', () => {
  console.log('\n\n🛑 SIGTERM received - closing connection...');
  ws.close();
});

console.log('\n⏳ Connecting to WebSocket...');
console.log('   Press Ctrl+C to stop the test');
