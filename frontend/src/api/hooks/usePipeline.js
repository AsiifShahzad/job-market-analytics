import { useQuery, useMutation } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import api from '@/api/client'

const STALE_TIME = 1 * 60 * 1000 // 1 minute

/**
 * Hook for fetching all pipeline runs
 */
export function usePipelineRuns() {
  return useQuery({
    queryKey: ['pipeline-runs'],
    queryFn: async () => {
      try {
        const response = await api.pipeline.runs()
        // Handle different API response structures
        const runsData =
          response?.data?.runs ||
          response?.runs ||
          response?.data ||
          []
        return {
          runs: Array.isArray(runsData) ? runsData : [],
          data: { runs: Array.isArray(runsData) ? runsData : [] },
        }
      } catch (error) {
        console.error('Error fetching pipeline runs:', error)
        return { runs: [], data: { runs: [] } }
      }
    },
    staleTime: STALE_TIME,
    retry: 2,
  })
}

/**
 * Hook for fetching pipeline run status with polling
 */
export function usePipelineStatus(runId, pollInterval = 3000) {
  return useQuery({
    queryKey: ['pipeline-status', runId],
    queryFn: () => api.pipeline.status(runId),
    staleTime: 0, // Always fetch fresh
    refetchInterval: pollInterval,
    enabled: runId !== null && runId > 0,
  })
}

/**
 * Hook for triggering a new pipeline run
 */
export function useTriggerPipeline() {
  return useMutation({
    mutationFn: () => api.pipeline.trigger(),
  })
}

/**
 * Hook for streaming pipeline logs via Server-Sent Events
 */
export function usePipelineLogsSSE(runId) {
  const [logs, setLogs] = useState([])
  const [isConnected, setIsConnected] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => {
    if (!runId || runId <= 0) {
      setLogs([])
      return
    }

    try {
      const eventSource = new EventSource(
        `${import.meta.env.VITE_API_URL}/api/pipeline/${runId}/logs`
      )

      eventSource.addEventListener('open', () => {
        setIsConnected(true)
      })

      eventSource.addEventListener('message', (event) => {
        const message = event.data
        setLogs((prev) => [...prev, message])
      })

      eventSource.addEventListener('error', () => {
        eventSource.close()
        setIsConnected(false)
        setError('SSE connection closed')
      })

      return () => {
        eventSource.close()
        setIsConnected(false)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'SSE connection failed')
    }
  }, [runId])

  return { logs, isConnected, error }
}
