// ==========================================
// グローバルスコープ（ファイル先頭）
// ==========================================

// --- IndexedDB & Auth Config ---
const DB_NAME = 'ToreKenDB';
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

// --- Helper for VAPID Key ---
function urlBase64ToUint8Array(base64String) {
    const padding = '='.repeat((4 - base64String.length % 4) % 4);
    const base64 = (base64String + padding)
        .replace(/-/g, '+')
        .replace(/_/g, '/');

    const rawData = window.atob(base64);
    const outputArray = new Uint8Array(rawData.length);

    for (let i = 0; i < rawData.length; ++i) {
        outputArray[i] = rawData.charCodeAt(i);
    }
    return outputArray;
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

// --- NotificationManager (from HanaView) ---
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
                // renderAllData not available here, but we can refresh
                // or show notification
                this.showInAppNotification('データが更新されました');
                location.reload();
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
                throw new Error('認証マネージャーが読み込まれていません。');
            }

            if (!AuthManager.isAuthenticated()) {
                console.warn('Cannot register push subscription: not authenticated');
                return;
            }

            console.log('📤 Sending push subscription to server...');

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
                if (toast.parentNode) document.body.removeChild(toast);
            }, 300);
        }, 3000);
    }
}

// ==========================================
// DOMContentLoaded以降
// ==========================================

document.addEventListener('DOMContentLoaded', () => {
    console.log("Tore-ken App Initializing...");

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
                // 少し待機してからNotificationManagerを初期化（iPhone PWA対策）
                await new Promise(resolve => setTimeout(resolve, 100));
                await globalNotificationManager.init();
                console.log('✅ Notifications initialized');
            } catch (error) {
                console.error('❌ Notification initialization failed:', error);
            }
        }

        if (!dashboardContainer.dataset.initialized) {
            console.log("Tore-ken Dashboard Initialized");
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

                // No Plotly rendering needed, image is loaded via HTML
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

        if (historyItem.status_text.includes("Red to")) badge.classList.add('status-green');
        else if (historyItem.status_text.includes("Green to")) badge.classList.add('status-red');
        else if (historyItem.status_text.includes("Green")) badge.classList.add('status-green');
        else if (historyItem.status_text.includes("Red")) badge.classList.add('status-red');
        else badge.classList.add('status-neutral');

        // Move Vertical Line on Image (CSS)
        const chartWrapper = document.getElementById('chart-wrapper');
        const cursor = document.getElementById('chart-cursor');

        if (chartWrapper && cursor && marketHistory.length > 0) {
            // Logic to calculate position
            // The chart image generated by mplfinance has margins.
            // With tight_layout and figsize(10,8), the plot area is roughly 80-90% of width.
            // This is a rough approximation. Ideally we'd know the exact pixel bounds.
            // Let's assume standard margins.

            // Adjust these offsets based on the actual generated image appearance
            const marginLeft = 0.05; // 5% from left
            const marginRight = 0.12; // 12% from right
            const plotWidthPct = 1.0 - marginLeft - marginRight;

            // Calculate percentage position
            // index 0 = left edge of plot area
            // index max = right edge of plot area
            const count = marketHistory.length;
            const pct = (index + 0.5) / count;
            const leftPos = (marginLeft + (pct * plotWidthPct)) * 100;

            cursor.style.left = `${leftPos}%`;
            cursor.style.display = 'block';
        }

        // Fetch Daily Data (Setup Stocks)
        const contentDiv = document.getElementById('setup-stocks-content');
        contentDiv.innerHTML = '<div class="loading-spinner-small"></div>';

        try {
            const response = await fetchWithAuth(`/api/daily/${dateKey}`);
            if (response.ok) {
                const data = await response.json();
                // Support both new 'setup_stocks' and old 'strong_stocks' keys
                renderSetupStocks(data.setup_stocks || data.strong_stocks);
            } else {
                // Check if this date is 'today' or close enough?
                // Or simply show no data
                contentDiv.innerHTML = '<p style="text-align: center; color: #757575;">No Setup Stocks data available for this date.</p>';
            }
        } catch (error) {
             contentDiv.innerHTML = '<p style="text-align: center; color: #757575;">Error loading data.</p>';
        }
    }

    function renderSetupStocks(stocks) {
        const contentDiv = document.getElementById('setup-stocks-content');
        if (!stocks || stocks.length === 0) {
            contentDiv.innerHTML = '<p style="text-align: center;">No Setup Stocks found.</p>';
            return;
        }

        // Match the reference image style
        // Image shows:
        // Universe: 5,626 Scanned: 5,557 Matches Found: 7 (We might not have these stats readily available unless passed from backend)
        // Then list of blocks: [ Ticker (RRS: ..., RVol: ..., ADR%: ...) ]

        // I will implement the list of blocks style.

        // Clear content
        contentDiv.innerHTML = '';

        // Matches Header
        const header = document.createElement('div');
        header.style.cssText = 'margin-bottom: 10px; font-size: 0.9em; text-align: right; color: #555;';
        header.textContent = `Matches Found: ${stocks.length}`;
        contentDiv.appendChild(header);

        // Container
        const listContainer = document.createElement('div');
        listContainer.style.cssText = 'display: flex; flex-direction: column; gap: 10px;';

        stocks.forEach(s => {
            const item = document.createElement('div');
            item.style.cssText = 'border: 2px solid black; padding: 10px; font-weight: bold; font-size: 0.7em; background: white;';

            // Ticker Line (Line 1)
            const line1 = document.createElement('div');
            const tickerSpan = document.createElement('span');
            tickerSpan.textContent = s.ticker;
            tickerSpan.style.cssText = 'font-size: 1.6em; cursor: pointer; text-decoration: underline; color: #000;';
            line1.appendChild(tickerSpan);

            // RealTime RVol (Magenta)
            const rvolSpan = document.createElement('span');
            rvolSpan.id = `rvol-${s.ticker}`;
            rvolSpan.style.cssText = 'color: #FF00FF; margin-left: 10px; font-size: 1.2em;';
            line1.appendChild(rvolSpan);

            // Indicators Line (Line 2)
            const line2 = document.createElement('div');
            line2.textContent = `RRS: ${s.rrs}, RVol: ${s.rvol}, ADR%: ${s.adr_pct}%`;
            line2.style.fontSize = '1.2em';

            // ATR% Multiple Line (Line 3)
            const line3 = document.createElement('div');
            line3.textContent = `ATR% Multiple from 50-MA: ${s.atr_multiple !== undefined ? s.atr_multiple : 'N/A'}`;
            line3.style.fontSize = '1.2em';

            // VCP Metrics Line (Line 4)
            const line4 = document.createElement('div');
            let vcpText = 'VCP Data: N/A';
            if (s.vcp_metrics) {
                const m = s.vcp_metrics;
                const contrStr = m.contractions ? `[${m.contractions.join(', ')}]` : '[]';
                const tightStr = m.is_tight ? 'Yes' : 'No';
                const dryUpStr = m.is_dry_up ? 'Yes' : 'No';
                const qualStr = m.vcp_qualified ? (m.qualified_date ? `Yes (${m.qualified_date.slice(5)})` : 'Yes') : 'No';
                vcpText = `Contr: ${contrStr} | Tight: ${tightStr} | DryUp: ${dryUpStr} | Qual (5d): ${qualStr}`;
            }
            line4.textContent = vcpText;
            line4.style.fontSize = '1.2em';
            line4.style.color = '#333';

            item.appendChild(line1);
            item.appendChild(line2);
            item.appendChild(line3);
            item.appendChild(line4);

            // Chart Container (Hidden by default)
            const chartContainer = document.createElement('div');
            chartContainer.style.display = 'none';
            chartContainer.style.marginTop = '10px';
            chartContainer.style.textAlign = 'center';

            // Toggle Logic
            tickerSpan.addEventListener('click', () => {
                const isHidden = chartContainer.style.display === 'none';
                chartContainer.style.display = isHidden ? 'block' : 'none';

                if (isHidden && !chartContainer.hasChildNodes()) {
                    // Load image
                    if (s.chart_image) {
                        const img = document.createElement('img');
                        img.alt = `${s.ticker} Chart`;
                        img.style.maxWidth = '100%';
                        img.style.border = '1px solid #ccc';

                        // Use authenticated fetch logic for image if needed, or simple img src if cookies work
                        // Since we implemented cookie-based auth for /api/stock-chart/, simple src should work
                        // IF the browser sends the cookie.
                        // However, we set the cookie with `SameSite=Strict` or `Lax`.
                        // Ideally, we can just use the src.
                        img.src = `/api/stock-chart/${s.chart_image}`;

                        // Handle error
                        img.onerror = () => {
                            chartContainer.innerHTML = '<span style="color:red; font-size:0.8em;">Failed to load chart.</span>';
                        };

                        chartContainer.appendChild(img);
                    } else {
                        chartContainer.textContent = 'Chart not available.';
                    }
                }
            });

            item.appendChild(chartContainer);
            listContainer.appendChild(item);
        });

        contentDiv.appendChild(listContainer);

        // Start polling for RealTime RVol if not already started
        if (!window.rvolInterval) {
            fetchRealTimeRVol(); // Initial fetch
            window.rvolInterval = setInterval(fetchRealTimeRVol, 60000); // Poll every 1 minute
        }
    }

    async function fetchRealTimeRVol() {
        try {
            const response = await fetchWithAuth('/api/realtime-rvol');
            if (response.ok) {
                const data = await response.json();
                Object.keys(data).forEach(ticker => {
                    const el = document.getElementById(`rvol-${ticker}`);
                    if (el) {
                        const val = data[ticker];
                        el.textContent = `RVol: ${val.toFixed(2)}x`;
                    }
                });
            }
        } catch (error) {
            console.error("Failed to fetch RealTime RVol:", error);
        }
    }

    // --- Auto Reload Function ---
    function setupAutoReload() {
        // 5-minute Force Reload (PWA)
        setInterval(() => {
            console.log("5-minute force reload triggered");
            location.reload();
        }, 300000); // 300,000 ms = 5 minutes

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
