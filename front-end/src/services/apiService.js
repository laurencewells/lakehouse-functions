import axios from 'axios';

const api = axios.create({
  });

if (import.meta.env.DEV) {
  api.defaults.baseURL = "http://localhost:8000/api/v1";
} else {
  api.defaults.baseURL = "/api/v1";
}
export const getFunctions = async () => {
    const response = await api.get('functions');
    return response.data;
}