import axios from 'axios'

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

const apiClient = axios.create({
  baseURL: `${API_BASE_URL}/api`,
  headers: {
    'Content-Type': 'application/json',
  },
})

/**
 * Typed fetch wrapper with error handling
 */
export const api = {
  /**
   * Generic GET request with query params
   */
  get: async (endpoint, params = {}, config = {}) => {
    try {
      const response = await apiClient.get(endpoint, {
        params,
        ...config,
      })
      return response.data
    } catch (error) {
      handleApiError(error)
      throw error
    }
  },

  /**
   * Generic POST request
   */
  post: async (endpoint, data = {}, config = {}) => {
    try {
      const response = await apiClient.post(endpoint, data, config)
      return response.data
    } catch (error) {
      handleApiError(error)
      throw error
    }
  },

  /**
   * Skills endpoints
   */
  skills: {
    list: async (params) => api.get('/skills', params),
    trend: async (skillName) => api.get(`/skills/${skillName}/trend`),
    cooccurrence: async (skillName) => api.get('/skills/cooccurrence', { skill: skillName }),
  },

  /**
   * Salary endpoints
   */
  salaries: {
    list: async (params) => api.get('/salaries', params),
    premium: async (params) => api.get('/salaries/skill-premium', params),
  },

  /**
   * Trends endpoints
   */
  trends: {
    emerging: async () => api.get('/trends/emerging'),
    heatmap: async (params) => api.get('/trends/heatmap', params),
  },

  /**
   * Insights endpoints
   */
  insights: {
    summary: async () => api.get('/insights/summary'),
    skills: async () => api.get('/insights/skills'),
    salary: async () => api.get('/insights/salary'),
    market: async () => api.get('/insights/market'),
    keywords: async () => api.get('/insights/keywords'),
    seniority: async () => api.get('/insights/seniority'),
  },

  /**
   * Pipeline endpoints
   */
  pipeline: {
    runs: async () => api.get('/pipeline/runs'),
    trigger: async () => api.post('/pipeline/trigger'),
    status: async (runId) => api.get(`/pipeline/${runId}/status`),
    logs: async (runId) => {
      return new EventSource(`${API_BASE_URL}/api/pipeline/${runId}/logs`)
    },
  },

  /**
   * Jobs endpoints
   */
  jobs: {
    search: async (params) => api.get('/jobs/search', params),
    detail: async (jobId) => api.get(`/jobs/${jobId}`),
    recommendations: async (params) => api.get('/jobs/recommendations', params),
    similar: async (jobId) => api.get(`/jobs/${jobId}/similar`),
    stats: async (params) => api.get('/jobs/stats', params),
  },
}

/**
 * Handle API errors with consistent messaging
 */
function handleApiError(error) {
  if (axios.isAxiosError(error)) {
    const status = error.response?.status || 500
    const message = error.response?.data?.detail || error.message
    return { status, message }
  }

  if (error instanceof Error) {
    return { status: 0, message: error.message }
  }

  return { status: 0, message: 'Unknown error occurred' }
}

export default api
