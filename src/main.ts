import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { CommandFactory } from 'nest-commander';
import { AppModule } from './app.module';
import { BigIntSerializerInterceptor } from './common/interceptors/bigint-serializer.interceptor';

async function bootstrap() {
  // Check if running as CLI command
  const isCommand = process.argv.some((arg) => ['hyperliquid:sync', 'trading:sync', 'pairs:sync'].includes(arg));

  if (isCommand) {
    // Run as CLI command
    await CommandFactory.run(AppModule, ['warn', 'error']);
  } else {
    // Run as HTTP server
    const app = await NestFactory.create(AppModule);

    // Enable CORS
    app.enableCors({
      origin: true,
      methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
      credentials: true,
    });

    // Global validation pipe
    app.useGlobalPipes(
      new ValidationPipe({
        transform: true,
        whitelist: true,
        forbidNonWhitelisted: false,
        transformOptions: {
          enableImplicitConversion: true,
        },
      }),
    );

    // Global interceptor to serialize BigInt and Decimal values
    app.useGlobalInterceptors(new BigIntSerializerInterceptor());

    // API prefix
    app.setGlobalPrefix('api');

    const port = process.env.PORT ?? 8001;
    await app.listen(port);

    console.log(`Application is running on: http://localhost:${port}/api`);
    console.log('Available endpoints:');
    console.log('  GET  /api/v1/wallets');
    console.log('  GET  /api/v1/wallets/stats');
    console.log('  GET  /api/v1/wallets/:address');
    console.log('  GET  /api/v1/wallets/:address/positions');
    console.log('  GET  /api/v1/wallets/:address/orders');
    console.log('  GET  /api/v1/wallets/:address/fills');
    console.log('  GET  /api/v1/wallets/:address/balances');
    console.log('  GET  /api/v1/wallets/:address/profile');
    console.log('  GET  /api/v1/wallets/:address/ledger');
    console.log('  POST /api/v1/wallets/:address/sync');
    console.log('  GET  /api/v1/wallets/positions/summary');
    console.log('  GET  /api/v1/wallets/activity/coin/:coin');
    console.log('  GET  /api/v1/transactions');
    console.log('  GET  /api/v1/transactions/:hash');
    console.log('  GET  /api/v1/sync-status');
    console.log('');
    console.log('CLI Commands (run with npm run start -- <command>):');
    console.log('  hyperliquid:sync [-f, --force]                              (runs continuously every 5 minutes)');
    console.log('  trading:sync [-a, --address <addr>] [-l, --limit <n>] [-f]  (runs continuously every 60 seconds)');
    console.log('  pairs:sync [-f, --force]');
  }
}

void bootstrap();
