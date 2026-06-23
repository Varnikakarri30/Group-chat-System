// State variables
let clientName = "Varnikahh";
let serverName = "harsha";
let clientSSE = null;
let serverSSE = null;

// UI Elements
const setupScreen = document.getElementById('setup-screen');
const workspaceScreen = document.getElementById('workspace-screen');
const setupForm = document.getElementById('setup-form');
const btnDisconnect = document.getElementById('btn-disconnect');

const clientNameInput = document.getElementById('client-name-input');
const serverNameInput = document.getElementById('server-name-input');

const clientTitleDisplay = document.getElementById('client-title-display');
const serverTitleDisplay = document.getElementById('server-title-display');
const clientTermName = document.getElementById('client-term-name');
const serverTermName = document.getElementById('server-term-name');

const clientStatus = document.getElementById('client-status');
const serverStatus = document.getElementById('server-status');
const clientPill = document.getElementById('client-pill');
const serverPill = document.getElementById('server-pill');

const clientMessages = document.getElementById('client-messages');
const serverMessages = document.getElementById('server-messages');

const clientMessageForm = document.getElementById('client-message-form');
const serverMessageForm = document.getElementById('server-message-form');
const clientMessageInput = document.getElementById('client-message-input');
const serverMessageInput = document.getElementById('server-message-input');

// Handle Setup Form Submit
setupForm.addEventListener('submit', (e) => {
    e.preventDefault();
    
    clientName = clientNameInput.value.trim() || "Varnikahh";
    serverName = serverNameInput.value.trim() || "harsha";
    
    // Update labels in workspace
    clientTitleDisplay.textContent = `Client: ${clientName}`;
    serverTitleDisplay.textContent = `Server: ${serverName}`;
    clientTermName.textContent = clientName;
    serverTermName.textContent = serverName;
    
    // Switch Screens
    setupScreen.classList.add('hidden');
    workspaceScreen.classList.remove('hidden');
    
    // Connect Streams
    connectClientStream();
    connectServerStream();
});

// Connect Client SSE Stream
function connectClientStream() {
    updateStatus(clientStatus, clientPill, 'connecting', 'Connecting...');
    
    const url = `/api/client/events?name=${encodeURIComponent(clientName)}`;
    clientSSE = new EventSource(url);
    
    clientSSE.onopen = () => {
        updateStatus(clientStatus, clientPill, 'connected', 'Client Stream: Active');
    };
    
    clientSSE.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            
            // Check if it's a join message (empty text) or standard message
            if (data.text) {
                appendMessage(clientMessages, 'inbound', data.server, data.text);
            }
        } catch (err) {
            console.error("Error parsing client message:", err);
        }
    };
    
    clientSSE.onerror = (err) => {
        console.error("Client SSE Error:", err);
        updateStatus(clientStatus, clientPill, 'disconnected', 'Client Stream: Error/Disconnected');
        clientSSE.close();
    };
}

// Connect Server SSE Stream
function connectServerStream() {
    updateStatus(serverStatus, serverPill, 'connecting', 'Connecting...');
    
    const url = `/api/server/events?name=${encodeURIComponent(serverName)}`;
    serverSSE = new EventSource(url);
    
    serverSSE.onopen = () => {
        updateStatus(serverStatus, serverPill, 'connected', 'Server Gateway: Active');
    };
    
    serverSSE.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            if (data.text) {
                appendMessage(serverMessages, 'inbound', data.client, data.text);
            }
        } catch (err) {
            console.error("Error parsing server message:", err);
        }
    };
    
    serverSSE.onerror = (err) => {
        console.error("Server SSE Error:", err);
        updateStatus(serverStatus, serverPill, 'disconnected', 'Server Gateway: Error/Disconnected');
        serverSSE.close();
    };
}

// Send Client Message
clientMessageForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const text = clientMessageInput.value.trim();
    if (!text) return;
    
    // Optimistically render outbound in Client panel
    appendMessage(clientMessages, 'outbound', clientName, text);
    clientMessageInput.value = '';
    
    try {
        const response = await fetch('/api/client/send', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ client: clientName, text: text })
        });
        
        if (!response.ok) {
            appendSystemMessage(clientMessages, 'System Error: Failed to deliver message to server.');
        }
    } catch (err) {
        console.error("Failed to send client message:", err);
        appendSystemMessage(clientMessages, 'System Error: Connection failed.');
    }
});

// Send Server Message
serverMessageForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const text = serverMessageInput.value.trim();
    if (!text) return;
    
    // Optimistically render outbound in Server panel
    appendMessage(serverMessages, 'outbound', serverName, text);
    serverMessageInput.value = '';
    
    try {
        const response = await fetch('/api/server/send', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ server: serverName, client: clientName, text: text })
        });
        
        if (!response.ok) {
            appendSystemMessage(serverMessages, `System Error: No active gRPC stream found for client '${clientName}'.`);
        }
    } catch (err) {
        console.error("Failed to send server message:", err);
        appendSystemMessage(serverMessages, 'System Error: Connection failed.');
    }
});

// Disconnect Demo
btnDisconnect.addEventListener('click', () => {
    disconnectAll();
    
    // Reset screens
    clientMessages.innerHTML = '';
    serverMessages.innerHTML = '';
    workspaceScreen.classList.add('hidden');
    setupScreen.classList.remove('hidden');
});

function disconnectAll() {
    if (clientSSE) {
        clientSSE.close();
        clientSSE = null;
    }
    if (serverSSE) {
        serverSSE.close();
        serverSSE = null;
    }
}

// Clean up connections if window closes
window.addEventListener('beforeunload', () => {
    disconnectAll();
});

// Helpers
function updateStatus(statusEl, pillEl, state, labelText) {
    const dot = statusEl.querySelector('.status-dot');
    const label = statusEl.querySelector('.status-label');
    
    // Reset dot classes
    dot.className = 'status-dot';
    pillEl.className = 'connection-status-pill';
    
    if (state === 'connecting') {
        dot.classList.add('pulse-red');
        pillEl.classList.add('red-glow');
        pillEl.textContent = 'CONNECTING';
    } else if (state === 'connected') {
        dot.classList.add('pulse-green');
        pillEl.classList.add('green-glow');
        pillEl.textContent = 'ACTIVE';
    } else {
        dot.classList.add('pulse-red');
        pillEl.classList.add('red-glow');
        pillEl.textContent = 'OFFLINE';
    }
    
    label.textContent = labelText;
}

function appendMessage(container, direction, sender, text) {
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
    
    container.appendChild(wrapper);
    scrollToBottom(container);
}

function appendSystemMessage(container, text) {
    const sysDiv = document.createElement('div');
    sysDiv.className = 'terminal-welcome fade-in';
    sysDiv.style.borderColor = 'rgba(239, 68, 68, 0.15)';
    sysDiv.innerHTML = `<span style="color: var(--red);">[ERROR] ${escapeHTML(text)}</span>`;
    container.appendChild(sysDiv);
    scrollToBottom(container);
}

function scrollToBottom(container) {
    // Scroll parent container (chat-console)
    const consoleEl = container.closest('.chat-console');
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
