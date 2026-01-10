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
                if (typeof renderAllData === 'function') {
                    renderAllData(event.data.data);
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
        // AuthManagerが存在するか確認（iPhone PWA対策）
        if (typeof AuthManager === 'undefined') {
            console.error('❌ AuthManager is not defined yet');
            throw new Error('認証マネージャーが読み込まれていません。ページを再読み込みしてください。');
        }

        if (!AuthManager.isAuthenticated()) {
            console.warn('Cannot register push subscription: not authenticated');
            return;
        }

        console.log('📤 Sending push subscription to server...');

        // fetchWithAuthも存在確認（念のため）
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

        // より詳細なエラーメッセージ
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
            // Service Workerの登録も解除
            if ('serviceWorker' in navigator) {
                const registrations = await navigator.serviceWorker.getRegistrations();
                for (let registration of registrations) {
                    await registration.unregister();
                }
            }
            alert('⚠️ 認証システムが更新されました。再度ログインしてください。');
            // ✅ ページをリロードしてService Workerを再登録
            location.reload();
            return; // これ以降の処理を実行しない
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

    function applyTabPermissions() {
        // Permissions logic removed as there is only one tab
        console.log("Applying permissions: All users see Market tab.");
    }

async function showDashboard() {
    if (authContainer) authContainer.style.display = 'none';
    if (dashboardContainer) dashboardContainer.style.display = 'block';

    applyTabPermissions();

    // NotificationManager初期化前に必要な依存関係を確認
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
            alert('⚠️ Push通知の登録に失敗しました。ページを再読み込みしてください。');
        }
    }

    if (!dashboardContainer.dataset.initialized) {
        console.log("HanaView Dashboard Initialized");
        initTabs();
        fetchDataAndRender();
        initSwipeNavigation();

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

    // --- Dashboard Functions ---

    async function fetchDataAndRender() {
        try {
            const response = await fetchWithAuth('/api/data');
            const data = await response.json();
            renderAllData(data);
        } catch (error) {
            if (error.message !== 'Authentication required') {
                console.error("Failed to fetch data:", error);
                document.getElementById('dashboard-content').innerHTML =
                    `<div class="card"><p>データの読み込みに失敗しました: ${error.message}</p></div>`;
            }
        }
    }

    // --- Tab-switching logic ---
    function initTabs() {
        const tabContainer = document.querySelector('.tab-container');
        tabContainer.addEventListener('click', (e) => {
            const button = e.target.closest('.tab-button');
            if (!button) return;

            const targetTab = button.dataset.tab;

            document.querySelectorAll('.tab-button').forEach(b => {
                const isActive = b.dataset.tab === targetTab;
                b.classList.toggle('active', isActive);
            });

            document.querySelectorAll('.tab-pane').forEach(p => {
                const isActive = p.id === `${targetTab}-content`;
                p.classList.toggle('active', isActive);
            });

            setTimeout(() => window.scrollTo(0, 0), 0);
        });
    }

    // --- Existing rendering functions ---
    function formatDateForDisplay(dateInput) {
        if (!dateInput) return '';
        try {
            const date = new Date(dateInput);
            if (isNaN(date.getTime())) return '';
            return `${date.getFullYear()}年${date.getMonth() + 1}月${date.getDate()}日 ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
        } catch (e) { return ''; }
    }

    function renderLightweightChart(containerId, data, title) {
        const container = document.getElementById(containerId);
        if (!container || !data || data.length === 0) {
            container.innerHTML = `<p>Chart data for ${title} is not available.</p>`;
            return;
        }
        container.innerHTML = '';

        const chart = LightweightCharts.createChart(container, {
            width: container.clientWidth,
            height: 300,
            layout: {
                backgroundColor: '#ffffff',
                textColor: '#333333'
            },
            grid: {
                vertLines: { color: '#e1e1e1' },
                horzLines: { color: '#e1e1e1' }
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal
            },
            timeScale: {
                borderColor: '#cccccc',
                timeVisible: true,
                secondsVisible: false
            },
            handleScroll: false,
            handleScale: false
        });

        const candlestickSeries = chart.addSeries(LightweightCharts.CandlestickSeries, {
            upColor: '#26a69a',
            downColor: '#ef5350',
            borderDownColor: '#ef5350',
            borderUpColor: '#26a69a',
            wickDownColor: '#ef5350',
            wickUpColor: '#26a69a'
        });

        const chartData = data.map(item => ({
            time: (new Date(item.time).getTime() / 1000),
            open: item.open,
            high: item.high,
            low: item.low,
            close: item.close
        }));

        candlestickSeries.setData(chartData);
        chart.timeScale().fitContent();

        new ResizeObserver(entries => {
            if (entries.length > 0 && entries[0].contentRect.width > 0) {
                chart.applyOptions({ width: entries[0].contentRect.width });
            }
        }).observe(container);
    }

    function renderMarketOverview(container, marketData, lastUpdated) {
        if (!container) return;
        container.innerHTML = '';
        const card = document.createElement('div');
        card.className = 'card';
        let content = '';
        if (marketData.fear_and_greed) { content += `<div class="market-section"><h3>Fear & Greed Index</h3><div class="fg-container" style="display: flex; justify-content: center; align-items: center; min-height: 400px;"><img src="/fear_and_greed_gauge.png?v=${new Date().getTime()}" alt="Fear and Greed Index Gauge" style="max-width: 100%; height: auto;"></div></div>`; }
        content += `<div class="market-grid"><div class="market-section"><h3>VIX (4h足)</h3><div class="chart-container" id="vix-chart-container"></div></div><div class="market-section"><h3>米国10年債金利 (4h足)</h3><div class="chart-container" id="t-note-chart-container"></div></div></div>`;
        if (marketData.ai_commentary) { const dateHtml = formatDateForDisplay(lastUpdated) ? `<p class="ai-date">${formatDateForDisplay(lastUpdated)}</p>` : ''; content += `<div class="market-section"><div class="ai-header"><h3>AI解説</h3>${dateHtml}</div><p>${marketData.ai_commentary.replace(/\n/g, '<br>')}</p></div>`; }
        card.innerHTML = content;
        container.appendChild(card);
        if (marketData.vix && marketData.vix.history) { renderLightweightChart('vix-chart-container', marketData.vix.history, 'VIX'); }
        if (marketData.t_note_future && marketData.t_note_future.history) { renderLightweightChart('t-note-chart-container', marketData.t_note_future.history, '10y T-Note'); }
    }

    function renderAllData(data) {
        console.log("Rendering all data:", data);
        const lastUpdatedEl = document.getElementById('last-updated');
        if (lastUpdatedEl && data.last_updated) { lastUpdatedEl.textContent = `Last updated: ${new Date(data.last_updated).toLocaleString('ja-JP')}`; }
        renderMarketOverview(document.getElementById('market-content'), data.market, data.last_updated);
    }

    // --- Swipe Navigation ---
    function initSwipeNavigation() {
        // Swipe navigation removed/simplified as there's only one tab
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
