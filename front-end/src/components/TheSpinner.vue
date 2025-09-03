<template>
    <div v-if="isLoading" class="spinner-border text-danger" role="status">
        <span class="visually-hidden">Loading...</span>
    </div>
    <div v-else>
        <slot name="content"></slot>
    </div>
</template>

<script setup>

const props = defineProps({
    isLoading: Boolean,
});

</script>

<style scoped>
.spinner-border {
    margin: 0 auto;
    display: block;
}
</style>
