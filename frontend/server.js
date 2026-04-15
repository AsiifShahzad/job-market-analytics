/**
 * Production Server for SPA
 * Handles routing fallback for React Router
 * Run: node server.js (or via Render's build commands)
 */

import express from 'express'
import path from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const app = express()
const PORT = process.env.PORT || 3000

// Serve static files from dist folder
app.use(express.static(path.join(__dirname, 'dist')))

// SPA Routing: All routes that aren't files serve index.html
app.get('*', (req, res) => {
  // Don't serve index.html for API routes (if any are on this server)
  if (req.path.startsWith('/api')) {
    return res.status(404).send('Not Found')
  }
  res.sendFile(path.join(__dirname, 'dist', 'index.html'))
})

app.listen(PORT, '0.0.0.0', () => {
  console.log(`✅ Frontend server running on http://0.0.0.0:${PORT}`)
  console.log(`   SPA routing configured - all routes serve index.html`)
})
