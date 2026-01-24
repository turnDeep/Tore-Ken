import pandas as pd
import numpy as np
import pandas_ta as ta
from scipy.signal import argrelextrema

class VCPConfig:
    """Configuration class for VCP Logic."""
    vcpRangePercentageFromTop = 20.0
    vcpLegsToCheckForConsolidation = 3
    vcpVolumeContractionRatio = 0.4
    enableAdditionalVCPFilters = True
    enableAdditionalVCPEMAFilters = True
    volumeRatio = 2.5

class VCPImplementation:
    """
    Ported VCP Logic.
    References:
    - geometric_logic (formerly validateVCP)
    - statistical_logic (formerly validateVCPMarkMinervini)
    """

    @staticmethod
    def getTopsAndBottoms(df, window=3, numTopsBottoms=6):
        """
        Identifies local tops and bottoms using argrelextrema.
        """
        if df is None or len(df) == 0:
            return None, None

        data = df.copy()
        data = data.fillna(0)
        data = data.replace([np.inf, -np.inf], 0)

        # Ensure 'Date' is a column if it's the index
        if 'Date' not in data.columns and isinstance(data.index, pd.DatetimeIndex):
            data.reset_index(inplace=True)
            data.rename(columns={"index": "Date"}, inplace=True)
        elif 'Date' not in data.columns:
             # If index is not datetime, try to use it anyway if useful, or create dummy
             data.reset_index(inplace=True)
             if 'index' in data.columns:
                 data.rename(columns={"index": "Date"}, inplace=True)

        data = data[data["High"] > 0]
        data = data[data["Low"] > 0]

        # Use High/Low columns (Case sensitive adjustment)
        highs = data["High"].values
        lows = data["Low"].values

        # Find indexes
        top_idxs = argrelextrema(highs, np.greater_equal, order=window)[0]
        bot_idxs = argrelextrema(lows, np.less_equal, order=window)[0]

        # Extract rows
        data["tops"] = np.nan
        data["bots"] = np.nan

        # We take the *first* numTopsBottoms from the result?
        # PKScreener code: `iloc[list(...[0])].head(numTopsBottoms)`
        # Note: argrelextrema returns indices sorted by position (time).
        # We likely want the *most recent* ones for analysis, but PKScreener takes `.head()`.
        # However, PKScreener often passes reversed data or handles it specifically.
        # In `validateVCP`, `data` was passed. In `getTopsAndBottoms`, `data` is used.
        # If `data` is chronological (old -> new), `head` takes oldest.
        # If `data` is reverse chronological (new -> old), `head` takes newest.
        # The internal logic of PKScreener sorts logic often.
        # Let's check `validateVCP` usage: `data["tops"] = ...head(4)`.
        # And `data["tops"]` logic in `getTopsAndBottoms`: `...head(numTopsBottoms)`.

        # IMPORTANT: PKScreener often reverses data `data = df[::-1]` before some checks,
        # but in `validateVCP`, `data = df.copy()` then `data.reset_index...`.
        # If `df` comes from `yfinance` it is usually Old->New.
        # If `getTopsAndBottoms` uses `.head()`, it might be getting the *oldest* tops if not reversed?
        # Let's look at `validateConsolidationContraction`: `tops, bots = self.getTopsAndBottoms(df=data...)`.
        # PKScreener's `StockScreener.py` sorts data descending (New->Old) at line 1130: `data = data.sort_index(ascending=False)`.
        # So `.head()` means *newest*.
        # We must ensure we pass Descending (New->Old) data or handle it.
        # Our `backend` usually uses Ascending (Old->New).
        # We will FLIP the dataframe here to match PKScreener's expectation of `.head()` being newest.

        # Flip to New->Old for calculation
        data_desc = data.sort_index(ascending=False).copy()
        highs_desc = data_desc["High"].values
        lows_desc = data_desc["Low"].values

        top_idxs_desc = argrelextrema(highs_desc, np.greater_equal, order=window)[0]
        bot_idxs_desc = argrelextrema(lows_desc, np.less_equal, order=window)[0]

        # Populate 'tops' and 'bots' columns in the descending DF
        # Note: argrelextrema indices correspond to the array passed (data_desc)

        # We need to map these back or just use the filtered dataframe.
        # The logic: `data["tops"] = data["High"].iloc[list(...)]...`

        # Let's create the tops/bots Series/DataFrame as PKScreener does
        tops_series = data_desc["High"].iloc[list(top_idxs_desc)].head(numTopsBottoms)
        bots_series = data_desc["Low"].iloc[list(bot_idxs_desc)].head(numTopsBottoms)

        data_desc.loc[tops_series.index, "tops"] = tops_series
        data_desc.loc[bots_series.index, "bots"] = bots_series

        tops = data_desc[data_desc["tops"] > 0].copy()
        bots = data_desc[data_desc["bots"] > 0].copy()

        return tops, bots

    @staticmethod
    def validateConsolidationContraction(df, legsToCheck=2, stockName=None):
        """
        Validates if the consolidation is contracting (VCP characteristic).
        """
        if df is None or len(df) == 0:
            return False, [], 0

        # PKScreener expects New->Old df here effectively because getTopsAndBottoms handles it or assumes it?
        # In PKScreener, `validateConsolidationContraction` calls `getTopsAndBottoms`.
        # We implemented `getTopsAndBottoms` to convert to Descending internally or expect it.
        # Let's assume input `df` is Ascending (standard). We will handle flipping.

        # Actually `getTopsAndBottoms` (our port) flips it.

        tops, bots = VCPImplementation.getTopsAndBottoms(df, window=5, numTopsBottoms=3*(legsToCheck if legsToCheck > 0 else 3))

        if tops is None or bots is None:
             return False, [], 0

        # PKScreener: `dfc = pd.concat([tops,bots],axis=0); dfc.sort_index(inplace=True)`
        # `dfc` becomes Ascending (Old->New) because datetime index sorts that way by default?
        # If `tops` and `bots` have DatetimeIndex, `sort_index` puts them in chronological order.

        dfc = pd.concat([tops, bots], axis=0)
        dfc.sort_index(ascending=True, inplace=True) # Ensure Old -> New for logic below

        # PKScreener logic iterates `index < indexLength-1`.
        # `top = dfc["tops"].iloc[index]`.
        # It drops indices where tops are not alternating properly or something?
        # "For a leg to form, we need two tops and one bottom \_/\_/\_/"
        # `if np.isnan(dfc["tops"].iloc[0]): dfc = dfc.tail(len(dfc)-1)` -> Ensures start with Top?

        dfc = dfc.assign(topbots=dfc[["tops", "bots"]].sum(axis=1))

        if dfc.empty:
            return False, [], 0

        if np.isnan(dfc["tops"].iloc[0]):
            dfc = dfc.iloc[1:]

        if dfc.empty:
            return False, [], 0

        # Logic to clean up consecutive tops or consecutive bottoms
        toBeDroppedIndices = []
        index = 0
        while index < len(dfc) - 1:
            top = dfc["tops"].iloc[index]
            top_next = dfc["tops"].iloc[index+1]
            bot = dfc["bots"].iloc[index]
            bot_next = dfc["bots"].iloc[index+1]

            if not np.isnan(top) and not np.isnan(top_next):
                if top >= top_next:
                    # Keep higher top? PKScreener: `indexVal = dfc[(dfc.Date == dfc["Date"].iloc[index+1])].index`
                    # It drops the *next* one if current >= next.
                    toBeDroppedIndices.append(dfc.index[index+1])
                else:
                    toBeDroppedIndices.append(dfc.index[index])

            if not np.isnan(bot) and not np.isnan(bot_next):
                if bot <= bot_next:
                    toBeDroppedIndices.append(dfc.index[index+1])
                else:
                    toBeDroppedIndices.append(dfc.index[index])
            index += 1

        dfc.drop(toBeDroppedIndices, axis=0, inplace=True, errors="ignore")

        consolidationPercentages = []
        index = 0
        # PKScreener iterates `while index < indexLength-3`
        # Because it looks at `index`, `index+1` (bot), `index+2` (top) -> 1 leg?

        relativeLegsTocheck = (legsToCheck if legsToCheck >= 3 else 3)

        # Note: dfc is Old->New.
        # Legs are formed by Top1 -> Bot -> Top2.
        # Consolidation = (Top - Bot) / Bot ?
        # PKScreener: `top = max(top1, top2); bot = ...; leg = (top-bot)/bot`

        while index < len(dfc) - 2: # Need at least 3 points: Top, Bot, Top
            try:
                top1 = dfc["tops"].iloc[index]
                # Next should be bot
                bot = dfc["bots"].iloc[index+1]
                # Next should be top
                top2 = dfc["tops"].iloc[index+2]

                # Check structure
                if np.isnan(top1) or np.isnan(bot) or np.isnan(top2):
                    index += 1
                    continue

                top = max(top1, top2)
                if bot != 0:
                    legConsolidation = int(round((top - bot) * 100 / bot, 0))
                else:
                    legConsolidation = 0

                consolidationPercentages.append(legConsolidation)
                if len(consolidationPercentages) >= relativeLegsTocheck:
                    break
                index += 2 # Skip to next Top pair
            except IndexError:
                break

        # Check for tightening
        # "Every next leg should be tighter than the previous one"
        # `consolidationPercentages` calculated from Old->New.
        # PKScreener: `consolidationPercentages = list(reversed(consolidationPercentages))`
        # So index 0 is Newest leg.

        consolidationPercentages = list(reversed(consolidationPercentages))
        devScore = 0

        if VCPConfig.enableAdditionalVCPFilters:
            if len(consolidationPercentages) >= 2:
                for i in range(legsToCheck - 1): # Check required legs
                    if i + 1 >= len(consolidationPercentages):
                        break
                    # Newest (i) should be <= Older (i+1)
                    if consolidationPercentages[i] > consolidationPercentages[i+1]:
                         # Strict tightening check
                         return False, consolidationPercentages[:relativeLegsTocheck], devScore

                    devScore += 2 - (consolidationPercentages[i] / consolidationPercentages[i+1] if consolidationPercentages[i+1] != 0 else 0)

        conditionMet = len(consolidationPercentages) >= legsToCheck
        return conditionMet, consolidationPercentages[:relativeLegsTocheck], devScore

    @staticmethod
    def geometric_logic(df, screenDict=None, saveDict=None, stockName=None, window=3, percentageFromTop=3):
        """
        Formerly validateVCP.
        Geometric structural analysis of VCP.
        """
        if df is None or len(df) == 0:
            return False

        data = df.copy()

        # 1. EMA Filter
        if VCPConfig.enableAdditionalVCPEMAFilters:
            # PKScreener uses New->Old for TA?
            # `ema = pktalib.EMA(reversedData["close"]...)` where `reversedData = data[::-1]`
            # If `data` passed here is Old->New (standard), then `reversedData` is New->Old.
            # `pktalib.EMA` expects series. `pandas_ta` expects Old->New usually.
            # PKScreener passes `reversedData['close']` (New->Old) to `pktalib.EMA`.
            # `pktalib.EMA` wraps `talib.EMA`. `talib` usually expects Old->New (time series).
            # Wait, if PKScreener passes New->Old to TA-Lib, the EMA calculation would be "backwards" in time?
            # Or does PKScreener internal logic assume Old->New?
            # In `StockScreener.py`, data is sorted Descending (New->Old).
            # `reversedData = data[::-1]` makes it Old->New (Ascending).
            # So `pktalib.EMA` receives Old->New (Correct for TA).

            # Our `df` is Old->New. So we don't need to reverse for TA.
            close_series = data["Close"]
            ema50 = ta.ema(close_series, length=50)
            ema20 = ta.ema(close_series, length=20)

            last_close = close_series.iloc[-1]
            last_ema50 = ema50.iloc[-1]
            last_ema20 = ema20.iloc[-1]

            if not (last_close >= last_ema50 and last_close >= last_ema20):
                return False

        percentageFromTop /= 100

        # Tops/Bots detection
        # We need New->Old for tops detection logic (head=newest) as implemented in getTopsAndBottoms
        # Our `getTopsAndBottoms` handles the flipping internally if we pass standard df?
        # No, `getTopsAndBottoms` flips `data_desc = data.sort_index(ascending=False)`.
        # So we pass standard `data` (Old->New).

        # But `geometric_logic` logic in PKScreener also calculates `tops` inline using `pktalib.argrelextrema`.
        # `data["tops"] = ... head(4)`.
        # Since `data` in PKScreener is New->Old, `head(4)` is newest 4 tops.
        # We need to replicate this "Newest Tops" logic.

        # Flip to New->Old for this check
        data_desc = data.sort_index(ascending=False).copy()
        highs_desc = data_desc["High"].values
        lows_desc = data_desc["Low"].values

        top_idxs = argrelextrema(highs_desc, np.greater_equal, order=window)[0]
        bot_idxs = argrelextrema(lows_desc, np.less_equal, order=window)[0]

        data_desc["tops"] = np.nan
        data_desc.iloc[top_idxs, data_desc.columns.get_loc("tops")] = data_desc["High"].iloc[top_idxs]

        # Head(4) newest tops
        tops = data_desc[data_desc["tops"] > 0].head(4)

        if tops.empty:
            return False

        highestTop = round(tops["High"].max(), 1)
        allTimeHigh = data["High"].max()

        # ATH Check
        withinATHRange = data["Close"].iloc[-1] >= (allTimeHigh - allTimeHigh * float(VCPConfig.vcpRangePercentageFromTop)/100)
        if not withinATHRange and VCPConfig.enableAdditionalVCPFilters:
            return False

        # Filtered Tops (Resistance Check)
        filteredTops = tops[tops["tops"] > (highestTop - (highestTop * percentageFromTop))]

        if len(filteredTops) == len(tops): # All recent tops are within range (Resistance)
            # Check Lows between tops
            lowPoints = []
            # Tops are in data_desc (New -> Old).
            # Loop `range(len(tops)-1)`: Top[i] is newer than Top[i+1].
            # Interval is between Top[i+1].Date (Start) and Top[i].Date (End).

            for i in range(len(tops) - 1):
                endDate = tops.index[i]
                startDate = tops.index[i+1]

                # Find min low in this range
                mask = (data.index >= startDate) & (data.index <= endDate)
                period_low = data.loc[mask, "Low"].min()
                lowPoints.append(period_low)

            if len(lowPoints) < 1:
                return False

            lowPointsOrg = list(lowPoints)
            lowPointsSorted = sorted(lowPoints, reverse=True) # Descending (High to Low)

            # "lowPointsOrg == lowPointsSorted" implies:
            # lowPoints[0] (Newest interval) is Highest?
            # Wait. PKScreener: `tops` is New->Old.
            # `i=0`: Top0 (Newest), Top1 (Older). Range Top1->Top0. `lowPoints[0]` is low between Top1 and Top0 (Newest Low).
            # `i=1`: Top1, Top2. Range Top2->Top1. `lowPoints[1]` is low between Top2 and Top1 (Older Low).
            # `lowPointsOrg` = [NewestLow, OlderLow, ...]
            # `lowPointsSorted` = [HighestVal, LowerVal, ...]
            # If `lowPointsOrg == lowPointsSorted`, it means NewestLow is Highest, OlderLow is Lower.
            # i.e. Lows are INCREASING (Higher Lows). Correct.

            ltp = data["Close"].iloc[-1]

            if (lowPointsOrg == lowPointsSorted and
                ltp < highestTop and
                ltp > lowPoints[0]):

                # Structural Contraction Check
                isTightening, consolidations, devScore = VCPImplementation.validateConsolidationContraction(
                    df,
                    legsToCheck=VCPConfig.vcpLegsToCheckForConsolidation if VCPConfig.enableAdditionalVCPFilters else 0,
                    stockName=stockName
                )

                if isTightening:
                    if screenDict is not None:
                        screenDict['vcp_pattern'] = "Geometric VCP"
                        screenDict['vcp_consolidations'] = consolidations
                    return True

        return False

    @staticmethod
    def statistical_logic(df, screenDict=None, saveDict=None):
        """
        Formerly validateVCPMarkMinervini.
        Trend & Volume Template analysis.
        """
        if df is None or len(df) == 0:
            return False

        data = df.copy()
        # Ensure Date index
        if 'Date' not in data.columns and isinstance(data.index, pd.DatetimeIndex):
             pass # Index is date
        else:
             # Handle if needed
             pass

        # Resample to Weekly
        # Map columns for aggregation
        ohlc_dict = {
            "Open": 'first',
            "High": 'max',
            "Low": 'min',
            "Close": 'last',
            "Volume": 'sum'
        }

        # PKScreener uses 'W' (Weekly frequency)
        weeklyData = data.resample('W').agg(ohlc_dict).dropna()

        if len(weeklyData) < 50: # Need enough data for 50SMA on weekly? Code uses 50.
             # Code uses w_sma_50 on weeklyData.
             return False

        # Current (Daily) Values
        recent_close = data["Close"].iloc[-1]

        # Weekly MAs (Old->New series)
        w_close = weeklyData["Close"]
        w_ema_13 = ta.ema(w_close, length=13)
        w_ema_26 = ta.ema(w_close, length=26)
        w_sma_50 = ta.sma(w_close, length=50)
        w_sma_40 = ta.sma(w_close, length=40)

        # Get latest values (iloc[-1])
        curr_w_ema_13 = w_ema_13.iloc[-1]
        curr_w_ema_26 = w_ema_26.iloc[-1]
        curr_w_sma_50 = w_sma_50.iloc[-1]
        curr_w_sma_40 = w_sma_40.iloc[-1]

        # Historical Weekly checks
        # w_sma_40_5w_ago = weeklyData.head(len-5)... but PKScreener code:
        # `w_sma_40_5w_ago = pktalib.SMA(weeklyData.head(len(weeklyData)-5)["close"],timeperiod=40).tail(1).iloc[0]`
        # This calculates SMA on a truncated series.
        # Ideally, we just take `w_sma_40.iloc[-6]`? (5 weeks ago from end).
        # SMA calculation shouldn't change much if we just index into the full calculated series,
        # unless it affects the window (it doesn't for SMA/EMA usually, except for start up).
        # PKScreener recalculates on truncated data. We will index into calculated series for efficiency.

        w_sma_40_5w_ago = w_sma_40.iloc[-6] if len(w_sma_40) >= 6 else w_sma_40.iloc[0]
        w_sma_40_10w_ago = w_sma_40.iloc[-11] if len(w_sma_40) >= 11 else w_sma_40.iloc[0]

        w_ema_26_20w_ago = w_ema_26.iloc[-21] if len(w_ema_26) >= 21 else w_ema_26.iloc[0]

        # Daily MAs checks
        # `recent_ema_13_20d_ago`. PKScreener: `EMA(reversedData.head(len-20)...)`.
        # `reversedData` is New->Old. `head(len-20)` removes Oldest 20?
        # Wait. `len(reversedData)-20`.
        # If I have 100 days. `reversedData` is D100, D99... D1.
        # `head(80)` is D100...D21.
        # `EMA` on that. `tail(1)` is D21.
        # So it effectively is EMA value 20 days ago (from Newest perspective).
        # We can just take `d_ema_13.iloc[-21]`.

        d_ema_13 = ta.ema(data["Close"], length=13)
        recent_ema_13_20d_ago = d_ema_13.iloc[-21] if len(d_ema_13) >= 21 else d_ema_13.iloc[0]

        recent_sma_50 = ta.sma(data["Close"], length=50).iloc[-1]

        # Price Position Checks
        # w_min_50 = min(1.3 * weeklyData.tail(50)["low"]) -> Price >= 1.3 * Low(52wk)
        # PKScreener uses `tail(50)` of Weekly. ~ 1 year.
        last_50_weeks = weeklyData.iloc[-50:]
        w_min_50 = last_50_weeks["Low"].min() * 1.3
        w_max_50 = last_50_weeks["High"].max() * 0.75 # Price >= 0.75 * High(52wk) (within 25% of high)

        # Volatility Check (Bollinger Band squeeze proxy?)
        # `(w_wma_8 - w_sma_8)*6/29 < 0.5`
        # PKScreener calculates WMA8 and SMA8 on Weekly.
        w_wma_8 = ta.wma(w_close, length=8).iloc[-1]
        w_sma_8 = ta.sma(w_close, length=8).iloc[-1]

        volatility_cond = abs(w_wma_8 - w_sma_8) * 6 / 29 < 0.5 # Using abs just in case, though code doesn't.

        # Volume Checks
        # "volInPreviousPullbacksShrinked"
        # Logic:
        # 1. Get recent 20 daily candles (`pullbackData`).
        # 2. Identify Pullbacks: Close < Open.
        # 3. `shrinkedVolData` = Pullbacks.
        # 4. `recentLargestVolume` = Max vol of NON-Pullbacks (Green days) in recent 3 days?
        #    `recentLargestVolume = max(pullbackData[pullbackData["PullBack"] == False].head(3)["volume"])`
        #    (Assuming PKScreener's `pullbackData` is New->Old)

        recent_20_days = data.iloc[-20:].sort_index(ascending=False) # New->Old

        # PKScreener: `pullbackData["close"].lt(pullbackData["open"])`
        is_pullback = recent_20_days["Close"] < recent_20_days["Open"]

        shrinkedVolData = recent_20_days[is_pullback]

        # Green days (Non-Pullback) in recent 3 days
        recent_3_days = recent_20_days.head(3)
        green_recent = recent_3_days[recent_3_days["Close"] >= recent_3_days["Open"]]

        recentLargestVolume = green_recent["Volume"].max() if not green_recent.empty else 0

        volInPreviousPullbacksShrinked = False
        if not shrinkedVolData.empty and recentLargestVolume > 0:
            # Check if ALL pullback volumes are < ratio * recentLargestVolume?
            # Code: `while index < len(shrinkedVolData): vol... = ... < ...; if not ... break`
            # Yes, ALL must be smaller.

            # Using max of shrinked to compare
            max_pullback_vol = shrinkedVolData["Volume"].max()
            if max_pullback_vol < (VCPConfig.vcpVolumeContractionRatio * recentLargestVolume):
                volInPreviousPullbacksShrinked = True
        elif shrinkedVolData.empty:
             # No pullbacks in last 20 days? Strong.
             volInPreviousPullbacksShrinked = True

        # Recent Volume Check
        # `recentLargestVolume >= volumeRatio * data["VolMA"]`
        # VolMA usually 20.
        vol_ma_20 = ta.sma(data["Volume"], length=20).iloc[-1]
        recentVolumeHasAboveAvgVol = recentLargestVolume >= (VCPConfig.volumeRatio * vol_ma_20)

        # Final Condition
        isVCP = (
            curr_w_ema_13 > curr_w_ema_26 and
            curr_w_ema_26 > curr_w_sma_50 and
            curr_w_sma_40 > w_sma_40_5w_ago and
            recent_close >= w_min_50 and
            recent_close >= w_max_50 and
            recent_ema_13_20d_ago > w_ema_26_20w_ago and
            w_sma_40_5w_ago > w_sma_40_10w_ago and
            recent_close > recent_sma_50 and
            volatility_cond and
            volInPreviousPullbacksShrinked and
            recentVolumeHasAboveAvgVol and
            recent_close > 10
        )

        if isVCP and screenDict is not None:
             screenDict['vcp_pattern'] = "Statistical VCP"

        return isVCP
