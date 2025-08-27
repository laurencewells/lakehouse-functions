<template>
  <nav class="nav-databricks navbar navbar-expand-lg navbar-light bg-light">
    <div class="container-fluid">
      <RouterLink class="navbar-brand" to="/">
        <img src="../assets/databricks-symbol-light.webp" alt="" width="24" height="24" class="d-inline-block align-text-top" />
        <span class="ms-2">
          Lakehouse Functions
        </span>
      </RouterLink>
      <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarSupportedContent" aria-controls="navbarSupportedContent" aria-expanded="false" aria-label="Toggle navigation">
        <span class="navbar-toggler-icon"></span>
      </button>
      <div class="collapse navbar-collapse" id="navbarSupportedContent">
        <ul class="navbar-nav me-auto mb-2 mb-lg-0">
          <li v-for="(route, index) in filteredRoutes" :key="index" class="nav-item">
          <RouterLink :to="route.path" class="nav-link">{{ route.name}}</RouterLink>
        </li>
        </ul>
      </div>
    </div>
  </nav>
</template>
  
<script setup>
import { RouterLink, useRouter } from 'vue-router'
const router = useRouter()

const filteredRoutes = router.options.routes.filter((r) => r.name !== 'home' && r.name !== 'table');
</script>

  <style scoped>
  /*
Defines the nav-databricks class
*/

.nav-databricks {
    background-color: #FF3621 !important;
  }
.nav-link, .navbar-brand {
    color: #F9F7F4 !important;
  }
.nav-link:disabled {
    color: #F9F7F4 !important;
  }

  </style>