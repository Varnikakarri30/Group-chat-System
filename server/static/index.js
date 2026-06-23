// Client Session State
let myUsername = "Varnikahh";
let myPasskey = "secret123";
let chatSSE = null;

// DOM Elements
const setupScreen = document.getElementById('setup-screen');
const workspaceScreen = document.getElementById('workspace-screen');
const setupForm = document.getElementById('setup-form');
const btnDisconnect = document.getElementById('btn-disconnect');

const usernameInput = document.getElementById('username-input');
const passkeyInput = document.getElementById('passkey-input');

const roomBadge = document.getElementById('room-badge');
const userBadge = document.getElementById('user-badge');
const termUsername = document.getElementById('term-username');
const termRoom = document.getElementById('term-room');

const chatStatus = document.getElementById('chat-status');
const chatPill = document.getElementById('chat-pill');
const chatMessages = document.getElementById('chat-messages');

const chatMessageForm = document.getElementById('chat-message-form');
const chatMessageInput = document.getElementById('chat-message-input');

// Setup form submit: Enter Chatroom
setupForm.addEventListener('submit', (e) => {
    e.preventDefault();
    
    myUsername = usernameInput.value.trim() || "Varnikahh";
    myPasskey = passkeyInput.value.trim() || "secret123";
    
    // Update headers and badges
    roomBadge.textContent = `ROOM: ${myPasskey}`;
    userBadge.textContent = `User: ${myUsername}`;
    termUsername.textContent = myUsername;
    termRoom.textContent = myPasskey;
    
    // UI Screen Swap
    setupScreen.classList.add('hidden');
    workspaceScreen.classList.remove('hidden');
    
    // Establish connection
    connectRoomStream();
});

// SSE Connection to chatroom
function connectRoomStream() {
    updateStatus('connecting', 'Stream: Connecting...');
    
    const url = `/api/chat/events?name=${encodeURIComponent(myUsername)}&passkey=${encodeURIComponent(myPasskey)}`;
    chatSSE = new EventSource(url);
    
    chatSSE.onopen = () => {
        updateStatus('connected', 'Stream: Active');
    };
    
    chatSSE.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            
            // 1. Skip if message text is empty (trigger joins)
            if (!data.text) return;
            
            // 2. Filter out own messages (already rendered optimistically)
            if (data.server === myUsername) return;
            
            // 3. Render Admin Broadcasts
            if (data.server === "[Admin Broadcast]") {
                appendAdminBroadcast(data.text);
            } else {
                // 4. Render inbound message from other group members
                appendMessage('inbound', data.server, data.text);
            }
        } catch (err) {
            console.error("Error reading room stream:", err);
        }
    };
    
    chatSSE.onerror = (err) => {
        console.error("SSE Error:", err);
        updateStatus('disconnected', 'Stream: Reconnecting/Offline');
    };
}

// Send Chat Message
chatMessageForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const text = chatMessageInput.value.trim();
    if (!text) return;
    
    // Optimistic UI - render outbound message immediately
    appendMessage('outbound', myUsername, text);
    chatMessageInput.value = '';
    
    try {
        const response = await fetch('/api/chat/send', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: myUsername, passkey: myPasskey, text: text })
        });
        
        if (!response.ok) {
            appendSystemMessage("System Error: Failed to transmit message to group.");
        }
    } catch (err) {
        console.error("Failed to post message:", err);
        appendSystemMessage("System Error: Server unreachable.");
    }
});

// Leave Chatroom
btnDisconnect.addEventListener('click', () => {
    disconnect();
    
    // Reset view state
    chatMessages.innerHTML = '';
    workspaceScreen.classList.add('hidden');
    setupScreen.classList.remove('hidden');
});

function disconnect() {
    if (chatSSE) {
        chatSSE.close();
        chatSSE = null;
    }
}

window.addEventListener('beforeunload', () => {
    disconnect();
});

// Interface state helpers
function updateStatus(state, labelText) {
    const dot = chatStatus.querySelector('.status-dot');
    const label = chatStatus.querySelector('.status-label');
    
    dot.className = 'status-dot';
    chatPill.className = 'connection-status-pill';
    
    if (state === 'connecting') {
        dot.classList.add('pulse-red');
        chatPill.classList.add('red-glow');
        chatPill.textContent = 'CONNECTING';
    } else if (state === 'connected') {
        dot.classList.add('pulse-green');
        chatPill.classList.add('green-glow');
        chatPill.textContent = 'ACTIVE';
    } else {
        dot.classList.add('pulse-red');
        chatPill.classList.add('red-glow');
        chatPill.textContent = 'OFFLINE';
    }
    
    label.textContent = labelText;
}

function appendMessage(direction, sender, text) {
    const wrapper = document.createElement('div');
    wrapper.className = `message-wrapper ${direction} fade-in`;
    
    const timeStr = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    
    wrapper.innerHTML = `
        <div class="msg-meta">
            <span class="msg-sender">${sender}</span>
            <span class="msg-time">${timeStr}</span>
        </div>
        <div class="message-bubble">
            ${escapeHTML(text)}
        </div>
    `;
    
    chatMessages.appendChild(wrapper);
    scrollToBottom();
}

function appendAdminBroadcast(text) {
    const wrapper = document.createElement('div');
    wrapper.className = 'message-wrapper admin-broadcast fade-in';
    
    wrapper.innerHTML = `
        <div class="message-bubble">
            📢 SYSTEM BROADCAST: ${escapeHTML(text)}
        </div>
    `;
    
    chatMessages.appendChild(wrapper);
    scrollToBottom();
}

function appendSystemMessage(text) {
    const sysDiv = document.createElement('div');
    sysDiv.className = 'terminal-welcome fade-in';
    sysDiv.style.borderColor = 'rgba(239, 68, 68, 0.15)';
    sysDiv.innerHTML = `<span style="color: var(--red);">[ERROR] ${escapeHTML(text)}</span>`;
    chatMessages.appendChild(sysDiv);
    scrollToBottom();
}

function scrollToBottom() {
    const consoleEl = document.getElementById('chat-console');
    if (consoleEl) {
        consoleEl.scrollTop = consoleEl.scrollHeight;
    }
}

function escapeHTML(str) {
    return str
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}
