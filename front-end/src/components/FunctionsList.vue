<template>
    <div class="mt-4">
    <h2>Available Functions</h2>
    
    <TheSpinner :is-loading="loading">
      <template #content>
        <div class="container-fluid px-0">
          <div class="row g-4">
            <div v-for="func in functions" :key="func.name + func.trigger.type" class="col-12 col-md-6 col-lg-4">
              <div class="card">
                <div class="card-body">
                  <div class="d-flex justify-content-between align-items-center mb-3">
                    <h5 class="card-title mb-0">{{ func.name }}</h5>
                    <span class="badge" :class="getBadgeClass(func.trigger.type)">{{ func.trigger.type }}</span>
                  </div>
                  <div class="card-text position-relative">
                    <template v-if="func.trigger.type === 'timer'">
                      <p class="mb-2"><i class="bi bi-clock me-2"></i><strong>Schedule:</strong> {{ formatCronSchedule(func.trigger.schedule) }}</p>
                    </template>
                    <template v-else-if="func.trigger.type === 'http'">
                      <p class="mb-2"><i class="bi bi-globe me-2"></i><strong>Method:</strong> {{ func.trigger.method }}</p>
                      <p class="mb-2"><i class="bi bi-link-45deg me-2"></i><strong>Endpoint:</strong> {{ func.trigger.endpoint }}</p>
                    </template>
                    <template v-else-if="func.trigger.type === 'unity_table'">
                      <p class="mb-2"><i class="bi bi-clock-history me-2"></i><strong>Check Interval:</strong> {{ func.trigger.check_interval }}s</p>
                      <p class="mb-2"><i class="bi bi-table me-2"></i><strong>Table Name: </strong> 
                        <template v-if="func.trigger.table_config">
                          <span class="table-part">{{ func.trigger.table_config.catalog }}</span>
                          <span class="table-separator">.</span>
                          <span class="table-part">{{ func.trigger.table_config.schema }}</span>
                          <span class="table-separator">.</span>
                          <span class="table-part">{{ func.trigger.table_config.name }}</span>
                        </template>
                        <template v-else>N/A</template> 
                      </p>
                    </template>
                    <span class="bg-secondary badge" @click="viewCode(func)">
                      <i class="bi bi-code-slash"></i>
                    </span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </template>
    </TheSpinner>

    <TheModal :is-open="showCodeModal" @modal-close="showCodeModal = false">
      <template #header>
        <h3 class="mb-0">{{ selectedFunctionName }} - Source Code</h3>
      </template>
      <template #content>
        <pre class="code-block"><code>{{ selectedFunctionCode }}</code></pre>
      </template>
      <template #footer>
        <button class="btn btn-secondary mt-2" @click="showCodeModal = false">Close</button>
      </template>
    </TheModal>
    </div>
</template>

<script setup>
import { ref, onMounted, reactive } from 'vue'
import { getFunctions } from '../services/apiService'
import TheSpinner from './TheSpinner.vue'
import TheModal from './TheModal.vue'
import { toast } from 'vue3-toastify'
import 'vue3-toastify/dist/index.css'

const functions = reactive([])
const loading = ref(true)
const showCodeModal = ref(false)
const selectedFunctionCode = ref('')
const selectedFunctionName = ref('')

const viewCode = (func) => {
  selectedFunctionCode.value = func.code
  selectedFunctionName.value = func.name
  showCodeModal.value = true
}

const getBadgeClass = (triggerType) => {
  const classes = {
    timer: 'bg-primary',
    http: 'bg-success',
    unity_table: 'bg-warning text-dark'
  }
  return classes[triggerType] || 'bg-secondary'
}

const formatCronSchedule = (cronExpression) => {
  // Handle common patterns
  if (cronExpression === '*/1 * * * *') return 'Every minute'
  if (cronExpression === '0/5 * * * *') return 'Every 5 minutes'
  if (cronExpression === '0/15 * * * *') return 'Every 15 minutes'
  if (cronExpression === '0/30 * * * *') return 'Every 30 minutes'
  if (cronExpression === '0 * * * *') return 'Every hour'
  if (cronExpression === '0 0 * * *') return 'Every day at midnight'
  if (cronExpression === '0 12 * * *') return 'Every day at noon'

  // Parse custom patterns
  const parts = cronExpression.split(' ')
  if (parts.length !== 5) return cronExpression

  const [minute, hour, dayOfMonth, month, dayOfWeek] = parts

  // Handle interval patterns
  if (minute.startsWith('*/')) {
    const interval = minute.substring(2)
    return `Every ${interval} minute${interval === '1' ? '' : 's'}`
  }

  // Handle specific time patterns
  if (minute.match(/^\d+$/) && hour.match(/^\d+$/)) {
    const formattedHour = parseInt(hour) % 12 || 12
    const ampm = parseInt(hour) >= 12 ? 'PM' : 'AM'
    const formattedMinute = minute.padStart(2, '0')
    return `Every day at ${formattedHour}:${formattedMinute} ${ampm}`
  }

  // Return original expression if pattern is not recognized
  return cronExpression
}

const loadFunctions = async () => {
  loading.value = true
  try {
    const response = await getFunctions()
    Object.assign(functions, response.functions)
  } catch (err) {
    toast.error('Failed to load functions. Please try again.')
    console.error('Error fetching functions:', err)
  }
  loading.value = false
}

onMounted(() => {
  loadFunctions()
})
</script>

<style scoped>
.functions-list {
  padding: 2rem;
}

.card {
  transition: transform 0.2s ease;
}

.card:hover {
  transform: translateY(-2px);
}

.code-block {
  background-color: #f8f9fa;
  padding: 1rem;
  border-radius: 4px;
  max-height: 500px;
  overflow-y: auto;
  white-space: pre-wrap;
  font-family: monospace;
  margin: 0;
}


.table-part {
  font-family: monospace;
  background-color: #f8f9fa;
  padding: 2px 4px;
  border-radius: 3px;
  color: #9b4c07;
  display: inline-block;
}

.table-separator {
  margin: 0 2px;
  color: #6c757d;
  font-weight: bold;
}
</style>