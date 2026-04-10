import { useQuery } from '@tanstack/react-query'
import api from '@/api/client'

const STALE_TIME = 5 * 60 * 1000 // 5 minutes

/**
 * Hook for fetching skills with filters
 */
export function useSkills(params) {
  return useQuery({
    queryKey: ['skills', params],
    queryFn: async () => {
      try {
        const response = await api.skills.list(params)
        // Handle different API response structures
        const skillsData =
          response?.data?.skills ||
          response?.skills ||
          response?.data ||
          []
        return {
          skills: Array.isArray(skillsData) ? skillsData : [],
          data: { skills: Array.isArray(skillsData) ? skillsData : [] },
        }
      } catch (error) {
        console.error('Error fetching skills:', error)
        return { skills: [], data: { skills: [] } }
      }
    },
    staleTime: STALE_TIME,
    keepPreviousData: true,
    retry: 2,
  })
}

/**
 * Hook for fetching skill trend data
 */
export function useSkillTrend(skillName) {
  return useQuery({
    queryKey: ['skill-trend', skillName],
    queryFn: () => api.skills.trend(skillName),
    staleTime: STALE_TIME,
    enabled: !!skillName,
  })
}

/**
 * Hook for fetching co-occurring skills
 */
export function useSkillCooccurrence(skillName) {
  return useQuery({
    queryKey: ['skill-cooccurrence', skillName],
    queryFn: () => api.skills.cooccurrence(skillName),
    staleTime: STALE_TIME,
    enabled: !!skillName,
  })
}

/**
 * Hook for fetching all unique skills for filtering
 */
export function useAllSkills() {
  return useQuery({
    queryKey: ['all-skills'],
    queryFn: async () => {
      const response = await api.skills.list({ limit: 500 })
      return response.skills
    },
    staleTime: STALE_TIME,
  })
}
