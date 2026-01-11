// ==========================================
// グローバルスコープ（ファイル先頭）
// ==========================================

// --- IndexedDB & Auth Config ---
const DB_NAME = 'HanaViewDB';
const DB_VERSION = 1;
const TOKEN_STORE_NAME = 'auth-tokens';

// --- Authentication Management (with IndexedDB support) ---
class AuthManager {
    static TOKEN_KEY = 'auth_token';
    static EXPIRY_KEY = 'auth_expiry';
    static PERMISSION_KEY = 'auth_permission';

    static async setTokenInDB(token) {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(DB_NAME, DB_VERSION);
            request.onerror = () => reject("Error opening DB for token storage");
            request.onupgradeneeded = event => {
                const db = event.target.result;
                if (!db.objectStoreNames.contains(TOKEN_STORE_NAME)) {
                    db.createObjectStore(TOKEN_STORE_NAME, { keyPath: 'id' });
                }
            };
            request.onsuccess = event => {
                const db = event.target.result;
                const transaction = db.transaction([TOKEN_STORE_NAME], 'readwrite');
                const store = transaction.objectStore(TOKEN_STORE_NAME);
                if (token) {
                    store.put({ id: 'auth_token', value: token });
                } else {
                    store.delete('auth_token');
                }
                transaction.oncomplete = () => resolve();
                transaction.onerror = () => reject("Error storing token in DB");
            };
        });
    }

    static async setAuthData(token, expiresIn, permission) {
        localStorage.setItem(this.TOKEN_KEY, token);
        const expiryTime = Date.now() + (expiresIn * 1000);
        localStorage.setItem(this.EXPIRY_KEY, expiryTime.toString());
        localStorage.setItem(this.PERMISSION_KEY, permission);
        try {
            await this.setTokenInDB(token);
            console.log(`Auth token and permission (${permission}) stored. Expires at:`, new Date(expiryTime).toLocaleString());
        } catch (error) {
            console.error("Failed to store token in IndexedDB:", error);
        }
    }

    static getToken() {
        const token = localStorage.getItem(this.TOKEN_KEY);
        const expiry = localStorage.getItem(this.EXPIRY_KEY);
        if (!token || !expiry || Date.now() > parseInt(expiry)) {
            if (token) this.clearAuthData();
            return null;
        }
        return token;
    }

    static getPermission() {
        return localStorage.getItem(this.PERMISSION_KEY);
    }

    static async clearAuthData() {
        localStorage.removeItem(this.TOKEN_KEY);
        localStorage.removeItem(this.EXPIRY_KEY);
        localStorage.removeItem(this.PERMISSION_KEY);
        try {
            await this.setTokenInDB(null);
            console.log('Auth data cleared from localStorage and IndexedDB');
        } catch (error) {
            console.error("Failed to clear token from IndexedDB:", error);
        }
    }

    static isAuthenticated() {
        return this.getToken() !== null;
    }

    static getAuthHeaders() {
        const token = this.getToken();
        return token ? { 'Authorization': `Bearer ${token}` } : {};
    }
}

// --- Authenticated Fetch Wrapper ---
async function fetchWithAuth(url, options = {}) {
    const authHeaders = AuthManager.getAuthHeaders();
    const response = await fetch(url, {
        ...options,
        headers: { ...options.headers, ...authHeaders }
    });

    if (response.status === 401) {
        console.log('Authentication failed (401), redirecting to auth screen');
        await AuthManager.clearAuthData();
        window.dispatchEvent(new CustomEvent('auth-required'));
        throw new Error('Authentication required');
    }
    return response;
}

// --- NotificationManager ---
class NotificationManager {
    constructor() {
        this.isSupported = 'Notification' in window && 'serviceWorker' in navigator && 'PushManager' in window;
        this.vapidPublicKey = null;
    }

    async init() {
        if (!this.isSupported) {
            console.log('Push notifications are not supported');
            return;
        }
        console.log('Initializing NotificationManager...');
        try {
            const response = await fetch('/api/vapid-public-key');
            const data = await response.json();
            this.vapidPublicKey = data.public_key;
            console.log('VAPID public key obtained');
        } catch (error) {
            console.error('Failed to get VAPID public key:', error);
            return;
        }
        const permission = await this.requestPermission();
        if (permission) {
            await this.subscribeUser();
        }
        navigator.serviceWorker.addEventListener('message', event => {
            if (event.data.type === 'data-updated' && event.data.data) {
                console.log('Data updated via push notification');
                // Reload dashboard logic
                if (typeof fetchDataAndRender === 'function') {
                    fetchDataAndRender();
                }
                this.showInAppNotification('データが更新されました');
            }
        });
    }

    async requestPermission() {
        const permission = await Notification.requestPermission();
        console.log('Notification permission:', permission);
        return permission === 'granted';
    }

    async subscribeUser() {
        try {
            const registration = await navigator.serviceWorker.ready;
            let subscription = await registration.pushManager.getSubscription();
            if (!subscription) {
                const convertedVapidKey = this.urlBase64ToUint8Array(this.vapidPublicKey);
                subscription = await registration.pushManager.subscribe({
                    userVisibleOnly: true,
                    applicationServerKey: convertedVapidKey
                });
            }
            await this.sendSubscriptionToServer(subscription);
            if ('sync' in registration) {
                await registration.sync.register('data-sync');
            }
        } catch (error) {
            console.error('Failed to subscribe user:', error);
        }
    }

    async sendSubscriptionToServer(subscription) {
    try {
        if (typeof AuthManager === 'undefined') {
            console.error('❌ AuthManager is not defined yet');
            throw new Error('認証マネージャーが読み込まれていません。ページを再読み込みしてください。');
        }

        if (!AuthManager.isAuthenticated()) {
            console.warn('Cannot register push subscription: not authenticated');
            return;
        }

        console.log('📤 Sending push subscription to server...');

        if (typeof fetchWithAuth === 'undefined') {
            console.error('❌ fetchWithAuth is not defined yet');
            throw new Error('通信機能が読み込まれていません。ページを再読み込みしてください。');
        }

        const response = await fetchWithAuth('/api/subscribe', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(subscription)
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Server returned ${response.status}: ${errorText}`);
        }

        const result = await response.json();
        console.log('✅ Push subscription registered:', result);
        this.showInAppNotification(`通知が有効になりました (権限: ${result.permission})`);
    } catch (error) {
        console.error('❌ Error sending subscription to server:', error);

        let errorMessage = error.message || '不明なエラー';
        if (error.message.includes('認証マネージャー') || error.message.includes('通信機能')) {
            errorMessage += '\n\niPhone PWAでこの問題が発生する場合：\n1. アプリを完全に終了\n2. Safariでページを開き直す\n3. 再度ホーム画面に追加';
        }

        alert(`⚠️ Push通知の登録に失敗しました:\n${errorMessage}`);
    }
}

    urlBase64ToUint8Array(base64String) {
        const padding = '='.repeat((4 - base64String.length % 4) % 4);
        const base64 = (base64String + padding)
            .replace(/\-/g, '+')
            .replace(/_/g, '/');
        const rawData = window.atob(base64);
        const outputArray = new Uint8Array(rawData.length);
        for (let i = 0; i < rawData.length; ++i) {
            outputArray[i] = rawData.charCodeAt(i);
        }
        return outputArray;
    }

    showInAppNotification(message) {
        const toast = document.createElement('div');
        toast.className = 'toast-notification';
        toast.textContent = message;
        toast.style.cssText = `
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: #006B6B;
            color: white;
            padding: 15px 20px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            z-index: 10000;
            animation: slideIn 0.3s ease-out;
        `;
        document.body.appendChild(toast);
        setTimeout(() => {
            toast.style.animation = 'slideOut 0.3s ease-out';
            setTimeout(() => {
                document.body.removeChild(toast);
            }, 300);
        }, 3000);
    }
}

// ==========================================
// DOMContentLoaded以降
// ==========================================

document.addEventListener('DOMContentLoaded', () => {
    console.log("HanaView App Initializing...");

    // --- DOM Element References ---
    const authContainer = document.getElementById('auth-container');
    const dashboardContainer = document.querySelector('.container');
    const pinInputsContainer = document.getElementById('pin-inputs');
    const pinInputs = pinInputsContainer ? Array.from(pinInputsContainer.querySelectorAll('input')) : [];
    const authErrorMessage = document.getElementById('auth-error-message');
    const authSubmitButton = document.getElementById('auth-submit-button');
    const authLoadingSpinner = document.getElementById('auth-loading');

    // --- State ---
    let failedAttempts = 0;
    const MAX_ATTEMPTS = 5;
    let globalNotificationManager = null;
    let marketHistory = [];
    let currentDateIndex = -1;

    // ✅ 認証エラーイベントのリスナー追加
    window.addEventListener('auth-required', () => {
        showAuthScreen();
    });

    // --- Main App Logic ---
    async function initializeApp() {
        // ✅ 古い認証データのクリーンアップ
        if (localStorage.getItem('auth_token') && !localStorage.getItem('auth_permission')) {
            console.log('🧹 Cleaning old authentication data...');
            await AuthManager.clearAuthData();
            if ('serviceWorker' in navigator) {
                const registrations = await navigator.serviceWorker.getRegistrations();
                for (let registration of registrations) {
                    await registration.unregister();
                }
            }
            alert('⚠️ 認証システムが更新されました。再度ログインしてください。');
            location.reload();
            return;
        }

        try {
            if (AuthManager.isAuthenticated()) {
                await showDashboard();
            } else {
                showAuthScreen();
            }
        } catch (error) {
            if (error.message !== 'Authentication required') {
                console.error('Error during authentication check:', error);
                if (authErrorMessage) authErrorMessage.textContent = 'サーバーとの通信に失敗しました。';
            }
            showAuthScreen();
        }
    }

    async function showDashboard() {
        if (authContainer) authContainer.style.display = 'none';
        if (dashboardContainer) dashboardContainer.style.display = 'block';

        if (typeof AuthManager === 'undefined' || typeof fetchWithAuth === 'undefined') {
            console.error('❌ Required dependencies not loaded. Skipping notification setup.');
            alert('⚠️ アプリの初期化に問題があります。ページを再読み込みしてください。');
            return;
        }

        if (!globalNotificationManager) {
            globalNotificationManager = new NotificationManager();
            try {
                await new Promise(resolve => setTimeout(resolve, 100));
                await globalNotificationManager.init();
                console.log('✅ Notifications initialized');
            } catch (error) {
                console.error('❌ Notification initialization failed:', error);
            }
        }

        if (!dashboardContainer.dataset.initialized) {
            console.log("HanaView Dashboard Initialized");
            fetchDataAndRender();
            dashboardContainer.dataset.initialized = 'true';
        }
    }

    function showAuthScreen() {
        if (authContainer) authContainer.style.display = 'flex';
        if (dashboardContainer) dashboardContainer.style.display = 'none';
        setupAuthForm();
    }

    function setupAuthForm() {
        if (!pinInputsContainer) return;
        pinInputs.forEach(input => { input.value = ''; input.disabled = false; });
        if(authSubmitButton) authSubmitButton.disabled = false;
        if(authErrorMessage) authErrorMessage.textContent = '';
        failedAttempts = 0;
        pinInputs[0]?.focus();

        pinInputs.forEach((input, index) => {
            input.addEventListener('input', () => {
                if (input.value.length === 1 && index < pinInputs.length - 1) {
                    pinInputs[index + 1].focus();
                }
            });
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Backspace' && input.value.length === 0 && index > 0) {
                    pinInputs[index - 1].focus();
                }
            });
            input.addEventListener('paste', (e) => {
                e.preventDefault();
                const pasteData = e.clipboardData.getData('text').trim();
                if (/^\d{6}$/.test(pasteData)) {
                    pasteData.split('').forEach((char, i) => { if (pinInputs[i]) pinInputs[i].value = char; });
                    handleAuthSubmit();
                }
            });
        });

        if (authSubmitButton) {
            const newButton = authSubmitButton.cloneNode(true);
            authSubmitButton.parentNode.replaceChild(newButton, authSubmitButton);
            newButton.addEventListener('click', handleAuthSubmit);
        }
    }

    async function handleAuthSubmit() {
        const pin = pinInputs.map(input => input.value).join('');
        if (pin.length !== 6) {
            if (authErrorMessage) authErrorMessage.textContent = '6桁のコードを入力してください。';
            return;
        }
        setLoading(true);
        try {
            const response = await fetch('/api/auth/verify', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ pin: pin })
            });
            const data = await response.json();
            if (response.ok && data.success) {
                await AuthManager.setAuthData(data.token, data.expires_in, data.permission);
                console.log('✅ Authentication complete, token saved');
                await showDashboard();
            } else {
                failedAttempts++;
                pinInputs.forEach(input => input.value = '');
                pinInputs[0].focus();
                if (failedAttempts >= MAX_ATTEMPTS) {
                    if (authErrorMessage) authErrorMessage.textContent = '認証に失敗しました。';
                    pinInputs.forEach(input => input.disabled = true);
                    document.getElementById('auth-submit-button').disabled = true;
                } else {
                    if (authErrorMessage) authErrorMessage.textContent = '正しい認証コードを入力してください。';
                }
            }
        } catch (error) {
            console.error('Error during PIN verification:', error);
            if (authErrorMessage) authErrorMessage.textContent = '認証中にエラーが発生しました。';
        } finally {
            setLoading(false);
        }
    }

    function setLoading(isLoading) {
        if (authLoadingSpinner) authLoadingSpinner.style.display = isLoading ? 'block' : 'none';
        const submitBtn = document.getElementById('auth-submit-button');
        if (submitBtn) submitBtn.style.display = isLoading ? 'none' : 'block';
    }

    // --- Dashboard Functions (Refactored) ---

    async function fetchDataAndRender() {
        try {
            const response = await fetchWithAuth('/api/market-analysis');
            if (!response.ok) throw new Error("Failed to load market analysis");

            const data = await response.json();

            if (data.history && data.history.length > 0) {
                marketHistory = data.history;
                currentDateIndex = marketHistory.length - 1; // Default to latest

                // Render Chart
                renderMarketChart(marketHistory);

                // Setup Controls
                setupControls();

                // Load Initial Daily Data
                updateDailyView(currentDateIndex);

                // Update Last Updated
                const lastUpdatedEl = document.getElementById('last-updated');
                if (lastUpdatedEl && data.last_updated) {
                    lastUpdatedEl.textContent = `Last updated: ${new Date(data.last_updated).toLocaleString('ja-JP')}`;
                }
            } else {
                console.error("No history data found");
            }
        } catch (error) {
            console.error("Failed to fetch market analysis:", error);
        }
    }

    function renderMarketChart(history) {
        const dates = history.map(h => h.date);
        const closes = history.map(h => h.close);
        const tsvs = history.map(h => h.tsv);
        const fastKs = history.map(h => h.fast_k);
        const slowDs = history.map(h => h.slow_d);

        // Background Shapes for Cycle Phases
        const shapes = [];
        let currentShape = null;

        history.forEach((h, i) => {
            let color = null;
            if (h.market_status === 'Green') color = 'rgba(135, 206, 235, 0.2)'; // SkyBlue
            else if (h.market_status === 'Red') color = 'rgba(240, 128, 128, 0.2)'; // LightCoral

            if (color) {
                if (currentShape && currentShape.fillcolor === color && i === currentShape.endIndex + 1) {
                    currentShape.endIndex = i;
                    currentShape.x1 = dates[i];
                } else {
                    if (currentShape) {
                        shapes.push({
                            type: 'rect',
                            xref: 'x', yref: 'paper',
                            x0: currentShape.x0, x1: currentShape.x1,
                            y0: 0, y1: 1,
                            fillcolor: currentShape.fillcolor,
                            line: {width: 0},
                            layer: 'below'
                        });
                    }
                    currentShape = {
                        x0: dates[i], x1: dates[i],
                        endIndex: i,
                        fillcolor: color
                    };
                }
            } else {
                if (currentShape) {
                     shapes.push({
                            type: 'rect',
                            xref: 'x', yref: 'paper',
                            x0: currentShape.x0, x1: currentShape.x1,
                            y0: 0, y1: 1,
                            fillcolor: currentShape.fillcolor,
                            line: {width: 0},
                            layer: 'below'
                        });
                    currentShape = null;
                }
            }
        });
        if (currentShape) {
             shapes.push({
                    type: 'rect',
                    xref: 'x', yref: 'paper',
                    x0: currentShape.x0, x1: currentShape.x1,
                    y0: 0, y1: 1,
                    fillcolor: currentShape.fillcolor,
                    line: {width: 0},
                    layer: 'below'
                });
        }

        const traceCandle = {
            x: dates,
            close: closes,
            high: history.map(h => h.high),
            low: history.map(h => h.low),
            open: history.map(h => h.open),
            type: 'candlestick',
            name: 'Price',
            xaxis: 'x',
            yaxis: 'y'
        };

        const traceTSV = {
            x: dates,
            y: tsvs,
            type: 'scatter',
            mode: 'lines',
            name: 'TSV',
            line: {color: 'teal', width: 1.5},
            xaxis: 'x',
            yaxis: 'y2'
        };

        const traceFastK = {
            x: dates,
            y: fastKs,
            type: 'scatter',
            mode: 'lines',
            name: 'Fast K',
            line: {color: 'cyan', width: 1.5},
            xaxis: 'x',
            yaxis: 'y3'
        };

        const traceSlowD = {
            x: dates,
            y: slowDs,
            type: 'scatter',
            mode: 'lines',
            name: 'Slow D',
            line: {color: 'orange', width: 1.5},
            xaxis: 'x',
            yaxis: 'y3'
        };

        const layout = {
            dragmode: 'pan', // Changed to pan for better mobile experience
            showlegend: false,
            grid: {rows: 3, columns: 1, pattern: 'independent', roworder: 'top to bottom'},
            xaxis: {rangeslider: {visible: false}, type: 'date'},
            yaxis: {title: 'Price', domain: [0.55, 1]},
            yaxis2: {title: 'TSV', domain: [0.3, 0.5]},
            yaxis3: {title: 'StochRSI', domain: [0, 0.25], range: [0, 100]},
            shapes: shapes,
            margin: {l: 40, r: 10, t: 30, b: 30},
            height: 450
        };

        Plotly.newPlot('market-chart', [traceCandle, traceTSV, traceFastK, traceSlowD], layout, {responsive: true});
    }

    function setupControls() {
        const slider = document.getElementById('date-slider');
        const prevBtn = document.getElementById('prev-btn');
        const nextBtn = document.getElementById('next-btn');

        slider.min = 0;
        slider.max = marketHistory.length - 1;
        slider.value = currentDateIndex;

        slider.addEventListener('input', (e) => {
            const idx = parseInt(e.target.value);
            updateDailyView(idx);
        });

        prevBtn.addEventListener('click', () => {
            if (currentDateIndex > 0) {
                updateDailyView(currentDateIndex - 1);
                slider.value = currentDateIndex;
            }
        });

        nextBtn.addEventListener('click', () => {
            if (currentDateIndex < marketHistory.length - 1) {
                updateDailyView(currentDateIndex + 1);
                slider.value = currentDateIndex;
            }
        });
    }

    async function updateDailyView(index) {
        currentDateIndex = index;
        const historyItem = marketHistory[index];
        const dateKey = historyItem.date_key; // YYYYMMDD

        // Update Date Display
        document.getElementById('selected-date').textContent = historyItem.date;

        // Update Status Badge
        const badge = document.getElementById('market-status-badge');
        badge.textContent = historyItem.status_text;
        badge.className = 'status-text'; // Reset

        if (historyItem.status_text.includes("Green")) badge.classList.add('status-green');
        else if (historyItem.status_text.includes("Red")) badge.classList.add('status-red');
        else badge.classList.add('status-neutral');

        // Move Vertical Line on Chart
        const verticalLine = {
            type: 'line',
            x0: historyItem.date,
            x1: historyItem.date,
            y0: 0,
            y1: 1,
            xref: 'x',
            yref: 'paper',
            line: {color: 'black', width: 1, dash: 'dot'}
        };

        // Preserve existing shapes
        const graphDiv = document.getElementById('market-chart');
        if (graphDiv && graphDiv.layout && graphDiv.layout.shapes) {
            const baseShapes = graphDiv.layout.shapes.filter(s => s.line.dash !== 'dot');
            const newShapes = [...baseShapes, verticalLine];
            Plotly.relayout('market-chart', {shapes: newShapes});
        }

        // Fetch Daily Data (Strong Stocks)
        const contentDiv = document.getElementById('strong-stocks-content');
        contentDiv.innerHTML = '<div class="loading-spinner-small"></div>';

        try {
            const response = await fetchWithAuth(`/api/daily/${dateKey}`);
            if (response.ok) {
                const data = await response.json();
                renderStrongStocks(data.strong_stocks);
            } else {
                contentDiv.innerHTML = '<p style="text-align: center; color: #757575;">No detailed data available for this date.</p>';
            }
        } catch (error) {
             contentDiv.innerHTML = '<p style="text-align: center; color: #757575;">Error loading data.</p>';
        }
    }

    function renderStrongStocks(stocks) {
        const contentDiv = document.getElementById('strong-stocks-content');
        if (!stocks || stocks.length === 0) {
            contentDiv.innerHTML = '<p style="text-align: center;">No Strong Stocks found.</p>';
            return;
        }

        let html = `
            <table class="strong-stocks-table">
                <thead>
                    <tr>
                        <th>Ticker</th>
                        <th>Price</th>
                        <th>RRS</th>
                        <th>RVol</th>
                        <th>ADR%</th>
                    </tr>
                </thead>
                <tbody>
        `;

        stocks.forEach(s => {
            html += `
                <tr>
                    <td style="font-weight:bold;">${s.Ticker}</td>
                    <td>${s.Close}</td>
                    <td>${s.RRS}</td>
                    <td>${s.RVol}</td>
                    <td>${s.ADR}%</td>
                </tr>
            `;
        });

        html += '</tbody></table>';
        contentDiv.innerHTML = html;
    }

    // --- Auto Reload Function ---
    function setupAutoReload() {
        const LAST_RELOAD_KEY = 'lastAutoReloadDate';
        setInterval(() => {
            const now = new Date();
            const day = now.getDay();
            const hours = now.getHours();
            const minutes = now.getMinutes();
            const isWeekday = day >= 1 && day <= 5;
            const isReloadTime = hours === 6 && minutes === 30;
            if (isWeekday && isReloadTime) {
                const today = now.toISOString().split('T')[0];
                const lastReloadDate = localStorage.getItem(LAST_RELOAD_KEY);
                if (lastReloadDate !== today) {
                    console.log('Auto-reloading page at 6:30 on a weekday...');
                    localStorage.setItem(LAST_RELOAD_KEY, today);
                    location.reload();
                }
            }
        }, 60000);
    }

    // --- App Initialization ---
    initializeApp();
    setupAutoReload();
});
