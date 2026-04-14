/**
 * React hook for fetching insights data from backend.
 * Handles API calls, caching, and error management.
 */

import { useQuery } from '@tanstack/react-query'
import { api } from '@/api/client.js'

/**
 * Hook for fetching complete insights summary
 */
export function useInsightsSummary() {
  return useQuery({
    queryKey: ['insights-summary'],
    queryFn: async () => {
      try {
        const response = await api.get('/insights/summary')
        return response.data || response
      } catch (error) {
        console.error('Error fetching insights summary:', error)
        return {
          summary: {},
          top_skills: [],
          trending_skills: [],
          salary_insights: {},
          market_insights: {},
          skill_insights: {},
          actionable_insights: [],
        }
      }
    },
    staleTime: 1000 * 60 * 5, // 5 minutes
    cacheTime: 1000 * 60 * 10, // 10 minutes
  })
}

/**
 * Hook for skill insights
 */
export function useSkillsInsights() {
  return useQuery({
    queryKey: ['insights-skills'],
    queryFn: async () => {
      try {
        const response = await api.get('/insights/skills')
        return response.data || response
      } catch (error) {
        console.error('Error fetching skill insights:', error)
        return { top_skills: [], trending_skills: [] }
      }
    },
    staleTime: 1000 * 60 * 5,
  })
}

/**
 * Hook for salary insights
 */
export function useSalaryInsights() {
  return useQuery({
    queryKey: ['insights-salary'],
    queryFn: async () => {
      try {
        const response = await api.get('/insights/salary')
        return response.data || response
      } catch (error) {
        console.error('Error fetching salary insights:', error)
        return { top_paying_skills: [], by_seniority: {}, remote_comparison: {} }
      }
    },
    staleTime: 1000 * 60 * 5,
  })
}

/**
 * Hook for market insights
 */
export function useMarketInsights() {
  return useQuery({
    queryKey: ['insights-market'],
    queryFn: async () => {
      try {
        const response = await api.get('/insights/market')
        return response.data || response
      } catch (error) {
        console.error('Error fetching market insights:', error)
        return {
          top_locations: [],
          remote_percentage: {},
          jobs_trend: [],
          seniority_distribution: {},
        }
      }
    },
    staleTime: 1000 * 60 * 5,
  })
}

/**
 * Hook for keyword insights
 */
export function useKeywordInsights() {
  return useQuery({
    queryKey: ['insights-keywords'],
    queryFn: async () => {
      try {
        const response = await api.get('/insights/keywords')
        return response.data || response
      } catch (error) {
        console.error('Error fetching keyword insights:', error)
        return { keywords_by_salary: [] }
      }
    },
    staleTime: 1000 * 60 * 5,
  })
}

/**
 * Hook for seniority insights
 */
export function useSeniorityInsights() {
  return useQuery({
    queryKey: ['insights-seniority'],
    queryFn: async () => {
      try {
        const response = await api.get('/insights/seniority')
        return response.data || response
      } catch (error) {
        console.error('Error fetching seniority insights:', error)
        return { distribution: {}, top_skills_in_senior_roles: [] }
      }
    },
    staleTime: 1000 * 60 * 5,
  })
}
