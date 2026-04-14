import { useQuery } from '@tanstack/react-query'
import api from '@/api/client'

const STALE_TIME = 5 * 60 * 1000 // 5 minutes

/**
 * Hook for fetching jobs with filters
 */
export function useJobs(filters) {
  const apiFilters = { ...filters }
  if (Array.isArray(apiFilters.skills)) {
    apiFilters.skills = apiFilters.skills.join(',')
  }
  return useQuery({
    queryKey: ['jobs', filters],
    queryFn: () => api.jobs.search(apiFilters),
    staleTime: STALE_TIME,
    keepPreviousData: true,
    enabled: !!filters,
  })
}

/**
 * Hook for fetching a single job by ID
 */
export function useJobDetail(jobId) {
  return useQuery({
    queryKey: ['job', jobId],
    queryFn: () => api.jobs.detail(jobId),
    staleTime: STALE_TIME,
    enabled: !!jobId,
  })
}

/**
 * Hook for fetching jobs with skill matching
 */
export function useJobsWithSkillsMatch(skills, skillMatchType = 'any', filters = {}) {
  return useQuery({
    queryKey: ['jobs-skills-match', skills, skillMatchType, filters],
    queryFn: () =>
      api.jobs.search({
        ...filters,
        skills: skills.length > 0 ? skills : undefined,
        skill_match_type: skills.length > 0 ? skillMatchType : undefined,
      }),
    staleTime: STALE_TIME,
    keepPreviousData: true,
    enabled: !!filters,
  })
}

/**
 * Hook for fetching job recommendations
 */
export function useJobRecommendations(params = {}) {
  return useQuery({
    queryKey: ['job-recommendations', params],
    queryFn: () => api.jobs.recommendations(params),
    staleTime: STALE_TIME,
  })
}

/**
 * Hook for fetching job statistics/analytics
 */
export function useJobStats(filters = {}) {
  return useQuery({
    queryKey: ['job-stats', filters],
    queryFn: () => api.jobs.stats(filters),
    staleTime: STALE_TIME * 2, // Cache longer for stats
  })
}

/**
 * Hook for fetching similar jobs
 */
export function useSimilarJobs(jobId) {
  return useQuery({
    queryKey: ['similar-jobs', jobId],
    queryFn: () => api.jobs.similar(jobId),
    staleTime: STALE_TIME,
    enabled: !!jobId,
  })
}
