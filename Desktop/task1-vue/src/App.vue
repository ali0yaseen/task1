<template>
  <main class="modal-mock">
    <TagHeader />
    <TagBody />
    <TagFooter />
  </main>
</template>

<script setup>
import TagHeader from './components/TagHeader.vue'
import TagBody from './components/TagBody.vue'
import TagFooter from './components/TagFooter.vue'
import 'bootstrap/dist/css/bootstrap.min.css'
import 'bootstrap-icons/font/bootstrap-icons.css'
</script>

<style>
:root {
  --surface: #ffffff;
  --line: rgba(0,0,0,0.08);
  --muted: #93A1AE;
  --title: #1b2430;
  --chipbg: #0601ae;
  --link: #0601ae;
  --cardbg: #f7f9fb;
}
body {
  background-color: #eef2f6;
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
  font-family: system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial;
}
.modal-mock {
  width: 600px;
  height: 864px;
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 14px;
  box-shadow: 0 18px 50px rgba(0,0,0,0.09);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
</style>
