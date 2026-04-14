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
      const response = await api.skills.list(params)
      // API returns { skills: [...], total_count, ... }
      const skills = Array.isArray(response?.skills) ? response.skills : []
      return { skills }
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

export function useAllSkills() {
  return useQuery({
    queryKey: ['all-skills'],
    queryFn: async () => {
      const response = await api.skills.list({ limit: 500 })
      const skills = Array.isArray(response?.skills) ? response.skills : []
      return { skills }
    },
    staleTime: STALE_TIME,
    retry: 2,
  })
}