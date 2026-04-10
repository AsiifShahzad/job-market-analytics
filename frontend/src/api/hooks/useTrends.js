import { useQuery } from '@tanstack/react-query'
import api from '@/api/client'

const STALE_TIME = 5 * 60 * 1000 // 5 minutes

/**
 * Hook for fetching emerging skills
 */
export function useEmergingSkills() {
  return useQuery({
    queryKey: ['trends-emerging'],
    queryFn: async () => {
      try {
        const response = await api.trends.emerging()
        // Handle different API response structures
        const emergingData =
          response?.data?.trending_skills ||
          response?.emerging_skills ||
          response?.data?.emerging_skills ||
          response?.trending_skills ||
          response?.data ||
          []
        return {
          emerging_skills: Array.isArray(emergingData) ? emergingData : [],
          data: { trending_skills: Array.isArray(emergingData) ? emergingData : [] },
        }
      } catch (error) {
        console.error('Error fetching emerging skills:', error)
        return { emerging_skills: [], data: { trending_skills: [] } }
      }
    },
    staleTime: STALE_TIME,
    retry: 2,
  })
}

/**
 * Hook for fetching heatmap data
 */
export function useHeatmap(params) {
  return useQuery({
    queryKey: ['trends-heatmap', params],
    queryFn: () => api.trends.heatmap(params),
    staleTime: STALE_TIME,
  })
}
