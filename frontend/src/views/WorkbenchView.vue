<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <section v-if="recoveryNotices.length" class="recovery-panel" data-testid="recovery-panel">
      <h3 class="recovery-title">中断恢复提示</h3>
      <ul class="recovery-list">
        <li v-for="notice in recoveryNotices" :key="notice.key" class="recovery-item" :data-testid="`recovery-${notice.key}`">
          {{ notice.text }}
        </li>
      </ul>
    </section>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">
            <template v-if="JSON_FIELDS.has(column)">
              <pre v-if="parseJsonCell(row[column])" class="json-cell">{{ formatJsonCell(row[column]) }}</pre>
              <span v-else>{{ row[column] ?? '—' }}</span>
            </template>
            <template v-else>{{ row[column] ?? '—' }}</template>
          </td>
          <td>
            <span :class="{ 'status-warn': isAttentionStatus(String(row.status)) }">{{ row.status }}</span>
          </td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  moduleMeta,
  runAction as applyAction,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

// 后端以 JSON 字符串承载的结构化字段，工作台需要格式化后再展示
const JSON_FIELDS = new Set(['消费分片', '源批信息'])
const ATTENTION_STATUSES = new Set(['落地中', '待清单确认', '存在差异', '未通过', '已失败'])

function parseJsonCell(value: unknown): Record<string, unknown> | unknown[] | null {
  if (typeof value !== 'string') {
    return null
  }
  const trimmed = value.trim()
  if (!trimmed.startsWith('{') && !trimmed.startsWith('[')) {
    return null
  }
  try {
    return JSON.parse(trimmed) as Record<string, unknown> | unknown[]
  } catch {
    return null
  }
}

function formatJsonCell(value: unknown): string {
  const parsed = parseJsonCell(value)
  return parsed === null ? String(value ?? '') : JSON.stringify(parsed, null, 2)
}

function isAttentionStatus(status: string): boolean {
  return ATTENTION_STATUSES.has(status)
}

// 值班人员重新进入工作台时的中断恢复视角：
// - 暂存落地：哪些分区清单未确认、已确认到第几片；
// - 到达对账：哪些请求在等新清单确认、自动续做时将消费哪些分片，以及旧分区已结算结果。
const recoveryNotices = computed(() => {
  const notices: { key: string; text: string }[] = []
  if (props.moduleKey === 'staging_area') {
    rows.value
      .filter((row) => String(row['清单状态']) === 'partial')
      .forEach((row, index) => {
        notices.push({
          key: `staging-${row.id}-${index}`,
          text: `分区 ${row['暂存编号']}（批次 ${row['批次编号']}，代际 ${row['代际']}）落地中断：已确认分片 ${row['已确认分片']}/${row['分片规划']}，清单 ${row['清单编号']} 待补齐末片后确认，恢复入口将自动续做。`,
        })
      })
  }
  if (props.moduleKey === 'arrival_reconcile') {
    rows.value
      .filter((row) => String(row.status) === '待清单确认')
      .forEach((row, index) => {
        const scope = parseJsonCell(row['消费分片']) as
          | { planned_shard_indexes?: number[]; consumable_shard_indexes?: number[] }
          | null
        notices.push({
          key: `recon-wait-${row.id}-${index}`,
          text: `对账 ${row['对账编号']}（批次 ${row['批次编号']}，代际 ${row['暂存代际']}）等待新清单确认后自动续做；规划分片 ${scope?.planned_shard_indexes?.join(',') || '—'}，当前可消费分片 ${scope?.consumable_shard_indexes?.join(',') || '无'}，恢复期间不产生回执。`,
        })
      })
    rows.value
      .filter((row) => ['已平账', '存在差异'].includes(String(row.status)) && row['回执编号'])
      .forEach((row, index) => {
        notices.push({
          key: `recon-done-${row.id}-${index}`,
          text: `对账 ${row['对账编号']} 已按代际 ${row['暂存代际']} 清单 ${row['清单编号']} 完成结算（${row.status}，实到 ${row['实到条数']}/${row['应到条数']}），业务回执 ${row['回执编号']}。`,
        })
      })
  }
  return notices
})

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)
</script>
