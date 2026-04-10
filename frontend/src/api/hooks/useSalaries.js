import { useQuery } from '@tanstack/react-query'
import api from '@/api/client'

const STALE_TIME = 5 * 60 * 1000 // 5 minutes

/**
 * Hook for fetching salary data with filters
 */
export function useSalaries(params) {
  return useQuery({
    queryKey: ['salaries', params],
    queryFn: () => api.salaries.list(params),
    staleTime: STALE_TIME,
    keepPreviousData: true,
  })
}

/**
 * Hook for fetching skill premium (salary impact)
 */
export function useSkillPremium(params) {
  return useQuery({
    queryKey: ['skill-premium', params],
    queryFn: () => api.salaries.premium(params),
    staleTime: STALE_TIME,
    keepPreviousData: true,
  })
}
