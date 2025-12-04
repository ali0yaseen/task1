<template>
  <header class="mm-header">
    <div class="d-flex align-items-center justify-content-between">
      <div class="crumb d-flex align-items-center">
        <i class="bi bi-tag"></i>
        <span>Tag Details</span>
      </div>
      <button class="btn btn-outline-secondary btn-sm">
        <i class="bi bi-x-lg me-1"></i> Close
      </button>
    </div>

    <div class="title-row d-flex align-items-center justify-content-between mt-2">
      <div class="d-flex align-items-center gap-2">
        <i class="bi bi-tag" style="color:#6c7a88;"></i>
        <h1 class="mb-0">{{ store.tagName }}</h1>
      </div>
      <button class="btn btn-primary btn-sm edit-btn" @click="editName">
        Edit tag information <i class="bi bi-arrow-right-short"></i>
      </button>
    </div>
  </header>
</template>

<script setup>
import { useTagStore } from '../stores/tagStore'
const store = useTagStore()

function editName() {
  const newName = prompt('Enter new tag name:', store.tagName)
  if (newName) store.updateTagName(newName)
}
</script>

<style scoped>
.mm-header { padding: 16px 18px 10px; background: var(--surface); border-bottom: 1px solid var(--line); }
.crumb { color:#6c7a88; font-weight:600; }
.title-row h1 { font-size: 1.45rem; margin: 0; color: var(--title); font-weight: 800; }
.edit-btn { background: var(--link); border-color: var(--link); }
</style>
