<template>
<div class="mt-4">
    <h2>Function Activity Feed</h2>
  <div class="card h-100">
    <div class="card-body">
      <TheSpinner :is-loading="loading">
        <template #content>
          <div>
            <div v-if="messages.length === 0" class="text-center text-muted py-5">
              <i class="bi bi-activity me-2"></i>No function activity yet...
            </div>
            <div v-else class="message-list">
              <div v-for="message in [...messages].reverse()" :key="message.id" class="alert alert-info mb-3">
                <div class="d-flex justify-content-between align-items-center mb-1">
                  <small class="text-muted">
                    <i class="bi bi-clock me-1"></i>{{ message.timestamp }}
                  </small>
                </div>
                <div class="message-text">{{ message.text }}</div>
              </div>
            </div>
          </div>
        </template>
      </TheSpinner>
    </div>
  </div>
</div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import { websocketService } from '../services/websocketService'
import TheSpinner from './TheSpinner.vue'

const messages = ref([])
const loading = ref(true)
let removeMessageHandler = null

onMounted(async () => {
  try {
    await websocketService.connect()
    removeMessageHandler = websocketService.onMessage((message) => {
      messages.value.push({
        id: Date.now(),
        text: message,
        timestamp: new Date().toLocaleTimeString()
      })
    })
  } catch (error) {
    console.error('Failed to connect to websocket:', error)
  } finally {
    loading.value = false
  }
})

onUnmounted(() => {
  if (removeMessageHandler) {
    removeMessageHandler()
  }
  websocketService.disconnect()
})
</script>

<style scoped>
.messages-container {
  min-height: 400px;
}

.message-list {
  max-height: 600px;
  overflow-y: auto;
}

.message-text {
  word-break: break-word;
}
</style>
