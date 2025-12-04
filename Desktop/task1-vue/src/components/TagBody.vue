<template>
  <section class="mm-body">
    <div class="card-pane">
      <div class="row meta-grid">
        <div class="col-7">
          <div class="field-label">Tag Name</div>
          <div class="field-value">{{ store.tagName }}</div>
        </div>
        <div class="col-5">
          <div class="field-label">Created Date</div>
          <div class="field-value">{{ store.createdDate }}</div>
        </div>
      </div>

      <div class="divider"></div>

      <div v-for="(list, label) in sections" :key="label" class="mb-2">
        <div class="section-title">{{ label }}</div>
        <div class="chips">
          <span class="chip" v-for="item in list" :key="item">{{ item }}</span>
        </div>
        <div class="divider"></div>
      </div>
    </div>
  </section>
</template>

<script setup>
import { computed } from 'vue'
import { useTagStore } from '../stores/tagStore'
const store = useTagStore()

const sections = computed(() => ({
  Accounts: store.accounts,
  Units: store.units,
  Drivers: store.drivers,
  Areas: store.areas
}))
</script>

<style scoped>
.mm-body { flex:1 1 auto; background:#fafbfc; padding:16px; overflow:auto; }
.card-pane { background: var(--cardbg); border: 1px solid rgba(0,0,0,0.06); border-radius:8px; padding:16px; }
.field-label { color: var(--muted); font-weight: 600; font-size: .92rem; }
.field-value { color:#2b3a48; font-weight: 600; }
.section-title { color: var(--muted); font-weight:700; font-size:.95rem; margin-bottom:8px; }
.chip { background: var(--chipbg); color:#fff; border-radius:10px; padding:6px 10px; font-weight:700; margin:4px; display:inline-block; }
.divider { border-top:1px solid var(--line); margin:14px 0; }
</style>
