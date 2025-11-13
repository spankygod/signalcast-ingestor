import dotenv from 'dotenv'
import postgres from 'postgres'

dotenv.config()

function readEnv(key: string): string | undefined {
  return process.env[key] ?? process.env[key.toLowerCase()] ?? process.env[key.toUpperCase()]
}

function buildConnectionStringFromParts(): string {
  const user = readEnv('DB_USER') ?? readEnv('user') ?? readEnv('PGUSER')
  const password = readEnv('DB_PASSWORD') ?? readEnv('password') ?? readEnv('PGPASSWORD')
  const host = readEnv('DB_HOST') ?? readEnv('host')
  const port = readEnv('DB_PORT') ?? readEnv('port') ?? '5432'
  const database = readEnv('DB_NAME') ?? readEnv('dbname') ?? readEnv('database') ?? 'postgres'
  const sslmode = readEnv('DB_SSLMODE') ?? readEnv('sslmode') ?? 'require'

  if (!user || !password || !host) {
    throw new Error('Missing DB connection pieces: provide DATABASE_URL or DB_* env vars')
  }

  const encodedUser = encodeURIComponent(user)
  const encodedPassword = encodeURIComponent(password)
  const query = sslmode ? `?sslmode=${encodeURIComponent(sslmode)}` : ''

  return `postgresql://${encodedUser}:${encodedPassword}@${host}:${port}/${database}${query}`
}

function getConnectionString(): string {
  const directUrl = readEnv('DATABASE_URL')
  if (directUrl) {
    return directUrl
  }

  return buildConnectionStringFromParts()
}

export async function testDbConnection(): Promise<void> {
  const sql = postgres(getConnectionString())

  try {
    await sql`select 1`
    console.log('Database connection succeeded')
  } catch (error) {
    console.error('Database connection failed')
    console.error(error)
    process.exitCode = 1
  } finally {
    await sql.end({ timeout: 5 })
  }
}

if (require.main === module) {
  testDbConnection().catch((error) => {
    console.error('Unexpected error while testing the database connection')
    console.error(error)
    process.exit(1)
  })
}
