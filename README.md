# Swarm MCP Server

MCP-сервер оркестрации роя Hermes. Предоставляет менеджеру инструмент
`order` для передачи приказов агентам через webhook API и ожидания ответа.

## Инструменты

| Tool | Описание |
|------|----------|
| `order(agent, command, wait_s)` | Отправить приказ агенту и дождаться ответа |

Агенты: `kb-organizer` (8645), `analyst` (8646), `contactor` (8647),
`secretary` (8644), `manager` (8648).

## Как работает order

1. Отправляет POST на webhook `/webhooks/manager-command` агента-адресата
2. Ждёт (по умолчанию 60с) пока у агента появится новая сессия
3. Читает последний ответ assistant из state.db агента
4. Возвращает ответ менеджеру

## Развёртывание

```bash
cp -r src /opt/swarm-mcp/
cd /opt/swarm-mcp && docker compose up -d --build
```

## Подключение к менеджеру

В `config.yaml` менеджера:

```yaml
mcp_servers:
  swarm:
    url: http://agent-sales-0:3004/mcp
    timeout: 300
    connect_timeout: 15
```
