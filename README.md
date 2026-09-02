# @lumen/queue

Fila de jobs **em memória** para Lumen, com retry, atraso (delay), prioridade e controle de concorrência.

```ts
import { MemoryQueue } from '@lumen/queue';
```

---

## MemoryQueue

```ts
const queue = new MemoryQueue({ concurrency: 1 });

// Registrar um handler
queue.process('send-email', async (job) => {
  await sendEmail(job.data.to, job.data.subject);
});

// Adicionar um job
await queue.add('send-email', { to: 'alice@example.com', subject: 'Hello' });

// Com atraso (5s)
await queue.add('send-email', { to: 'bob@example.com' }, { delay: 5000 });

// Com retry (até 3 tentativas)
await queue.add('send-email', { to: 'charlie@example.com' }, { attempts: 3 });
```

### Construtor
- `new MemoryQueue({ concurrency })` — `concurrency` (padrão `1`): jobs processados em paralelo ao mesmo tempo.

---

## Métodos

| Método | Descrição |
| --- | --- |
| `add(name, data, options?)` | Enfileira um job e retorna o `Job` criado. |
| `process(name, handler)` | Registra o handler para um tipo de job. |
| `getJob(id)` | Busca um job (ativo, concluído ou falho). |
| `getWaiting()` / `getActive()` / `getCompleted()` / `getFailed()` | Listas de jobs por estado. |
| `removeJob(id)` | Remove um job da fila. |
| `close()` | Para o processamento e limpa a fila. |

---

## `JobOptions`

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `delay?` | `0` | Atraso em ms antes de processar. |
| `attempts?` | `1` | Número máximo de tentativas (retry). |
| `priority?` | `0` | Maior valor = processado primeiro. |

---

## `Job`

```ts
interface Job<T extends JobData = JobData> {
  id: string;
  name: string;
  data: T;
  status: JobStatus;
  attempts: number;
  maxAttempts: number;
  createdAt: number;
  processedAt?: number;
  completedAt?: number;
  failedAt?: number;
  error?: string;
  delay?: number;
}
```

### `JobStatus`
`'pending' | 'active' | 'completed' | 'failed' | 'delayed'`.

### Tipos auxiliares
- `JobData = Record<string, unknown>` — payload do job.
- `JobHandler = (job) => Promise<void>` — assinatura do handler.

---

## Comportamento

- **Retry**: se o handler lançar erro e `attempts < maxAttempts`, o job volta para `pending`.
- **Sem handler registrado**: o job é marcado como `failed` com erro `No handler registered for "<name>"`.
- **Delay**: jobs entram como `delayed` e são liberados após o atraso via `setTimeout`.

---

## Limitações

- **Em memória** e por instância (perde-se ao reiniciar; não é distribuída).
- Não há persistência, filas nomeadas ou agendamento recorrente.
