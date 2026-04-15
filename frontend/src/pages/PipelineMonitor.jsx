import React from 'react'
import {
  usePipelineRuns,
  usePipelineStatus,
  useTriggerPipeline,
  usePipelineLogsSSE,
} from '@/api/hooks/usePipeline.js'
import { formatDateTime, formatTimeAgo, formatPipelineStatus } from '@/utils/formatters.js'
import { FaSpinner, FaPlay, FaExclamation, FaCheck, FaClock } from 'react-icons/fa6'

export default function PipelineMonitor() {
  const [liveRunId, setLiveRunId] = React.useState(null)
  const [showLivePanel, setShowLivePanel] = React.useState(false)

  const { data: runsData, isLoading: runsLoading, refetch: refetchRuns } = usePipelineRuns()
  const triggerMutation = useTriggerPipeline()

  const handleTriggerPipeline = async () => {
    try {
      const result = await triggerMutation.mutateAsync()
      setLiveRunId(result.run_id)
      setShowLivePanel(true)
      refetchRuns()
    } catch (error) {
      console.error('Failed to trigger pipeline:', error)
    }
  }

  return (
    <div className="min-h-screen bg-gray-50 px-4 md:px-6 py-8">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">Pipeline Monitor</h1>
            <p className="text-gray-600 mt-1">Track ETL pipeline execution status</p>
          </div>
          <button
            onClick={handleTriggerPipeline}
            disabled={triggerMutation.isPending}
            className="flex items-center gap-2 px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 disabled:opacity-50 disabled:cursor-not-allowed transition"
          >
            {triggerMutation.isPending ? (
              <>
                <FaSpinner className="w-4 h-4 animate-spin" />
                Triggering...
              </>
            ) : (
              <>
                <FaPlay className="w-4 h-4" />
                Trigger Run
              </>
            )}
          </button>
        </div>

        {/* Info Banner */}
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-3 sm:p-4\">
          <h3 className="font-semibold text-sm sm:text-base text-blue-900 mb-2\">Job Quality Validation</h3>
          <ul className="text-xs sm:text-sm text-blue-800 space-y-1 list-disc list-inside\">
            <li><strong>Filtered:</strong> Jobs rejected due to: being non-real listings (e.g., "system design"), missing required fields, insufficient description length, invalid title format, or duplicate detection</li>
            <li><strong>Inserted:</strong> Only verified real job postings from legitimate companies</li>
            <li>Each job is validated for: authentic job role keywords, minimum description quality, reasonable title length, and semantic uniqueness</li>
          </ul>
        </div>

        {/* Runs Table */}
        <div className="bg-white rounded-lg shadow overflow-hidden\">
          <div className="overflow-x-auto\">
            <table className="w-full text-sm\">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className=\"text-left py-2 sm:py-3 px-2 sm:px-4 font-semibold text-xs sm:text-sm text-gray-700\">Run ID</th>
                  <th className=\"text-left py-2 sm:py-3 px-2 sm:px-4 font-semibold text-xs sm:text-sm text-gray-700\">Started</th>
                  <th className=\"text-left py-2 sm:py-3 px-2 sm:px-4 font-semibold text-xs sm:text-sm text-gray-700\">Finished</th>
                  <th className=\"text-left py-2 sm:py-3 px-2 sm:px-4 font-semibold text-xs sm:text-sm text-gray-700\">Status</th>
                  <th className=\"text-right py-2 sm:py-3 px-2 sm:px-4 font-semibold text-xs sm:text-sm text-gray-700\">Fetched</th>
                  <th className=\"text-right py-2 sm:py-3 px-2 sm:px-4 font-semibold text-xs sm:text-sm text-gray-700\">Filtered</th>
                  <th className=\"text-right py-2 sm:py-3 px-2 sm:px-4 font-semibold text-xs sm:text-sm text-gray-700\">Inserted</th>
                </tr>
              </thead>
              <tbody>
                {runsLoading ? (
                  <tr>
                    <td colSpan={6} className="py-12 text-center text-gray-500">
                      <FaSpinner className="w-6 h-6 animate-spin inline mr-2" />
                      Loading runs...
                    </td>
                  </tr>
                ) : (runsData?.runs || []).length === 0 ? (
                  <tr>
                    <td colSpan={6} className="py-12 text-center text-gray-500">
                      No pipeline runs yet
                    </td>
                  </tr>
                ) : (
                  (runsData?.runs || []).map((run) => {
                    const status = formatPipelineStatus(run.status)
                    return (
                      <tr
                        key={run.id}
                        className="border-b border-gray-200 hover:bg-gray-50 cursor-pointer"
                        onClick={() => {
                          if (run.status === 'RUNNING') {
                            setLiveRunId(run.id)
                            setShowLivePanel(true)
                          }
                        }}
                      >
                        <td className="py-4 px-4 text-gray-900 font-medium">#{run.id}</td>
                        <td className="py-4 px-4 text-gray-600">
                          {formatDateTime(run.started_at || run.created_at)}
                        </td>
                        <td className="py-4 px-4 text-gray-600">
                          {run.completed_at ? formatDateTime(run.completed_at) : '-'}
                        </td>
                        <td className="py-4 px-4">
                          <span className={`inline-block px-3 py-1 rounded-full text-xs font-semibold ${status.color}`}>
                            {status.label}
                          </span>
                        </td>
                        <td className="py-4 px-4 text-right text-gray-900 font-medium">
                          {run.jobs_fetched || 0}
                        </td>
                        <td className="py-4 px-4 text-right text-orange-600 font-medium" title="Jobs filtered out (non-real jobs, invalid format, etc.)">
                          {run.jobs_skipped || 0}
                        </td>
                        <td className="py-4 px-4 text-right text-gray-900 font-medium">
                          {run.jobs_inserted || 0}
                        </td>
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* Live Status Panel */}
      {showLivePanel && liveRunId && (
        <LiveStatusPanel
          runId={liveRunId}
          onClose={() => {
            setShowLivePanel(false)
            setLiveRunId(null)
          }}
          onFinished={() => {
            refetchRuns()
          }}
        />
      )}
    </div>
  )
}

function LiveStatusPanel({ runId, onClose, onFinished }) {
  const { data: statusData, isLoading } = usePipelineStatus(
    runId,
    3000 // Poll every 3 seconds
  )
  const { logs, isConnected, error: sseError } = usePipelineLogsSSE(runId)
  const logsEndRef = React.useRef(null)

  // Auto-scroll to bottom
  React.useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [logs])

  // Check if run is finished
  React.useEffect(() => {
    if (statusData?.status === 'SUCCESS' || statusData?.status === 'FAILED') {
      onFinished()
    }
  }, [statusData])

  const status = statusData || { status: 'LOADING', jobs_fetched: 0, jobs_skipped: 0, jobs_inserted: 0 }
  const statusInfo = formatPipelineStatus(status.status)
  const isRunning = status.status === 'RUNNING'

  return (
    <div className="fixed inset-0 z-50 bg-black bg-opacity-50 flex items-end md:items-center justify-center p-4">
      <div className="bg-white rounded-lg shadow-xl max-w-3xl w-full max-h-96 md:max-h-80 flex flex-col">
        {/* Header */}
        <div className="border-b border-gray-200 p-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <h3 className="font-semibold text-lg text-gray-900">Run #{runId}</h3>
            <span className={`inline-block px-3 py-1 rounded-full text-xs font-semibold ${statusInfo.color}`}>
              {statusInfo.label}
            </span>
          </div>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-gray-700 text-2xl leading-none"
          >
            ×
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-hidden flex flex-col md:flex-row gap-4 p-4">
          {/* Metrics */}
          <div className="md:w-48 space-y-3">
            <div>
              <p className="text-sm text-gray-600">Status</p>
              <p className="text-lg font-semibold text-gray-900">{statusInfo.label}</p>
            </div>
            <div>
              <p className="text-sm text-gray-600">Jobs Fetched</p>
              <p className="text-lg font-semibold text-gray-900">{status.jobs_fetched}</p>
            </div>
            <div>
              <p className="text-sm text-gray-600">Jobs Skipped</p>
              <p className="text-lg font-semibold text-orange-600">{status.jobs_skipped || 0}</p>
            </div>
            <div>
              <p className="text-sm text-gray-600">Jobs Inserted</p>
              <p className="text-lg font-semibold text-gray-900">{status.jobs_inserted}</p>
            </div>
            <div>
              <p className="text-sm text-gray-600">Unique Skills</p>
              <p className="text-lg font-semibold text-gray-900">{status.unique_skills || 0}</p>
            </div>

            {/* Progress */}
            {isRunning && status.percentage_complete !== undefined && (
              <div>
                <p className="text-sm text-gray-600 mb-2">Progress</p>
                <div className="w-full bg-gray-200 rounded-full h-2">
                  <div
                    className="bg-purple-600 h-2 rounded-full transition-all"
                    style={{ width: `${status.percentage_complete}%` }}
                  />
                </div>
                <p className="text-xs text-gray-600 mt-1">{status.percentage_complete}%</p>
              </div>
            )}

            {status.error_message && (
              <div className="bg-red-50 border border-red-200 rounded p-2">
                <p className="text-xs text-red-700">{status.error_message}</p>
              </div>
            )}
          </div>

          {/* Logs */}
          <div className="flex-1 bg-gray-900 rounded p-3 font-mono text-sm text-gray-100 overflow-y-auto max-h-64 md:max-h-full">
            {isConnected ? (
              <>
                <div className="text-green-400">$ Connected to pipeline stream</div>
                {logs.length === 0 ? (
                  <div className="text-gray-500">Waiting for logs...</div>
                ) : (
                  logs.map((log, idx) => (
                    <div key={idx} className="text-gray-200">
                      {log}
                    </div>
                  ))
                )}
                <div ref={logsEndRef} />
              </>
            ) : sseError ? (
              <div className="text-yellow-400">
                SSE not available - using polling instead
                {logs.length > 0 && (
                  <>
                    <div>Last update: {logs[logs.length - 1]}</div>
                  </>
                )}
              </div>
            ) : (
              <div className="text-gray-500">Initializing stream...</div>
            )}
          </div>
        </div>

        {/* Footer */}
        <div className="border-t border-gray-200 p-4 flex justify-end gap-2">
          {!isRunning && (
            <button
              onClick={onClose}
              className="px-4 py-2 bg-gray-200 text-gray-900 rounded-lg hover:bg-gray-300 transition"
            >
              Close
            </button>
          )}
        </div>
      </div>
    </div>
  )
}
