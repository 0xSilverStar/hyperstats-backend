type Env = {
  DATABASE_URL: string;
};

const env: Env = {
  DATABASE_URL: process.env.DATABASE_URL || 'postgresql://user:password@localhost:5432/mydb',
};

export default env;
