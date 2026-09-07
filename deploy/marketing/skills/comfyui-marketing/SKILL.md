---
name: comfyui-marketing
description: Use for marketing background and illustration generation through the installed ComfyUI REST service, checking GPU availability, reproducing an image from a seed, or troubleshooting designer image generation.
---

# ComfyUI для Рисоваки

Сервис на хосте atlant-llm0: `http://192.168.122.1:8188` из marketing VM/контейнера. Это не localhost и не 1С/VoIP. Конфиг: `/opt/data/comfyui.json`; клиент: `/opt/data/bin/comfyui-client.py` (Python stdlib). CLI `comfy` не нужен.

1. Выполни безопасную проверку без генерации:
   `python3 /opt/data/bin/comfyui-client.py`
   Она проверяет `/system_stats`, `/queue`, `/object_info`, checkpoint и поля графа. Это не доказательство успешной генерации.
2. Подтверждённый checkpoint: `sd_xl_base_1.0.safetensors`; graph SDXL: CheckpointLoaderSimple → CLIPTextEncode + EmptyLatentImage → KSampler → VAEDecode → SaveImage. По умолчанию 512×512, 12 steps, Euler/normal, CFG6, seed42. Модели хранятся на хосте `/opt/comfyui/models`, результат `/opt/comfyui/output`.
3. Только для назначенного разрешённого фона/иллюстрации:
   `python3 /opt/data/bin/comfyui-client.py --generate --seed 42 --prompt 'Abstract blue geometric background, no text or logo'`
   Клиент не отправляет задание при занятой очереди или свободной VRAM менее 8GiB. GPU общий с LM Studio: не останавливай контейнеры, не выгружай модели и не вызывай `/free` ради картинки. При блокере попроси владельца инфраструктуры выделить GPU-окно. Порог — предосторожность, не гарантия отсутствия OOM.
4. Результаты, точный API graph и history сохраняются в `/opt/data/inbound/comfyui/<prompt_id>/`. Seed и граф помогают повторить результат; побитовая воспроизводимость зависит от версии сервера, GPU и checkpoint. Проверь PNG визуально, размеры, соответствие брифу. При timeout проверь историю существующего prompt_id, не отправляй повторно вслепую.

Текущий endpoint доступен без API-ключа в внутренней сети. Не публикуй его наружу. Если администратор включит Bearer-auth, клиент читает `COMFYUI_API_KEY` из окружения (секрет не хранить в скиле); при 401 запроси доступ, не обходи защиту.

Не генерируй логотипы и фирменный текст; используй одобренные активы и отдельную типографику. Генерация не равна согласованию/публикации. Доставляй проверенные файлы через штатный `telegram_reply.files`, не обещай результат до проверки.
