<script setup>
import FunctionsList from '../components/FunctionsList.vue'
import FunctionFeed from '../components/FunctionFeed.vue'
</script>

<template>
  <main>
    <div class="row">
      <FunctionsList />
    </div>
    <div class="row">
      <FunctionFeed />
    </div>
  </main>
</template>

<style scoped>
main {
  padding: 1rem;
}
</style>
