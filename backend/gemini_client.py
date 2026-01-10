"""
Gemini API Client for HanaView
google-genai ライブラリを使用した統一クライアント
"""

import os
import logging
from typing import Optional
from google import genai

logger = logging.getLogger(__name__)

class GeminiClient:
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            # Don't raise error on init, just log warning.
            # This allows app to start even without API key (features will be disabled)
            logger.warning("GEMINI_API_KEY environment variable is not set")
            self.client = None
        else:
            self.client = genai.Client(api_key=self.api_key)

        self.model = 'gemini-2.0-flash-exp' # Using 2.0 Flash as per latest info/spec recommendation if available, or fallback to what spec said.
        # Spec said 'gemini-3-flash-preview' but that might be a typo in spec or future model.
        # Spec says: `gemini-3-flash-preview` (2025年12月時点の最新モデル).
        # Since I am in 2024/2025 context, I should stick to what spec says or a safe default.
        # Let's stick to the spec's model name but be ready to fallback.
        self.model = 'gemini-2.0-flash-exp' # Actually spec said gemini-3-flash-preview but usually it's better to use stable.
        # Wait, the spec explicitly said "gemini-3-flash-preview". I will use that.
        self.model = 'gemini-3-flash-preview'

    def generate_content(self, prompt: str, max_retries: int = 3) -> Optional[str]:
        """
        Gemini APIでコンテンツを生成

        Args:
            prompt: プロンプトテキスト
            max_retries: リトライ回数

        Returns:
            生成されたテキスト、失敗時はNone
        """
        if not self.client:
            logger.error("Gemini Client not initialized (missing API key)")
            return None

        for attempt in range(max_retries):
            try:
                response = self.client.models.generate_content(
                    model=self.model,
                    contents=prompt
                )

                if response.text:
                    return response.text
                else:
                    logger.warning(f"Empty response from Gemini API (attempt {attempt + 1}/{max_retries})")

            except Exception as e:
                logger.error(f"Gemini API error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    logger.error("All Gemini API attempts failed")
                    return None

        return None

# グローバルインスタンス
gemini_client = GeminiClient()
