import { describe, expect, it } from 'bun:test';
import { execFile } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { promisify } from 'node:util';

const execute = promisify(execFile);
const require = createRequire(import.meta.url);

// Exercise the immutable Git source selected by CI, not its built checkout.
// The application pins the exact verified head; this verification PR is not merged.
describe.skipIf(process.env.GITHUB_ACTIONS !== 'true')(
  'Git package distribution',
  () => {
    it.each(['production', 'development'])(
      'installs the fixed Git source under NODE_ENV=%s',
      async (mode) => {
        const eventPath = process.env.GITHUB_EVENT_PATH;
        if (!eventPath) throw new Error('GitHub event payload is required');
        const event = JSON.parse(await readFile(eventPath, 'utf8')) as {
          pull_request?: { head: { sha: string; repo: { clone_url: string } } };
          repository?: { clone_url: string };
          after?: string;
        };
        const head =
          process.env.GITHUB_EVENT_NAME === 'pull_request'
            ? event.pull_request?.head
            : ['push', 'workflow_dispatch'].includes(
                  process.env.GITHUB_EVENT_NAME ?? '',
                ) && event.repository
              ? {
                  sha:
                    process.env.GITHUB_EVENT_NAME === 'push'
                      ? event.after
                      : process.env.GITHUB_SHA,
                  repo: event.repository,
                }
              : undefined;
        if (!head?.sha || !/^[a-f0-9]{40}$/.test(head.sha)) {
          throw new Error(
            'Distribution verification requires an immutable PR or push head',
          );
        }
        if (
          !/^https:\/\/github\.com\/[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\.git$/.test(
            head.repo.clone_url,
          )
        ) {
          throw new Error('Unexpected source repository');
        }

        const directory = await mkdtemp(join(tmpdir(), 'matador-git-'));
        try {
          const pnpm = join(
            dirname(require.resolve('pnpm/package.json')),
            'bin/pnpm.cjs',
          );
          const source = `${head.repo.clone_url}#${head.sha}&path:/packages/matador`;
          await writeFile(
            join(directory, 'package.json'),
            JSON.stringify({
              name: 'matador-distribution-check',
              private: true,
              dependencies: {
                '@zdavison/matador': '4.2.0',
                '@proone/external-integration-matador': source,
              },
            }),
          );
          await writeFile(
            join(directory, 'pnpm-workspace.yaml'),
            "allowBuilds:\n  '@zdavison/matador@4.2.0-proone.1': true\n",
          );
          const userConfig = join(directory, 'empty.npmrc');
          await writeFile(userConfig, '');
          const env: NodeJS.ProcessEnv = {
            ...process.env,
            NODE_ENV: mode,
            npm_config_userconfig: userConfig,
          };
          for (const key of [
            'NPM_TOKEN',
            'NODE_AUTH_TOKEN',
            'GITHUB_NPM_TOKEN',
            'NPM_CONFIG_TOKEN',
          ]) {
            delete env[key];
          }
          const options = {
            cwd: directory,
            env,
            timeout: 240_000,
            maxBuffer: 4 * 1024 * 1024,
          };
          const installArgs = [
            pnpm,
            'install',
            '--store-dir',
            join(directory, 'store'),
          ];
          await execute(
            'node',
            [...installArgs, '--no-frozen-lockfile'],
            options,
          );
          const lock = await readFile(
            join(directory, 'pnpm-lock.yaml'),
            'utf8',
          );
          expect(lock).toContain(head.sha);
          expect(lock).toContain(
            "'@zdavison/matador@4.2.0':\n    resolution: {integrity: sha512-uq2oGOkmyLiQFi9mVQyfXeMp4ofzrtVZjgNs+V5MwiFVesrQLDmtuL0gzlP5AbisOHerlTZCDzccUMOlzD1xFw==}",
          );
          await rm(join(directory, 'node_modules'), {
            recursive: true,
            force: true,
          });
          await rm(join(directory, 'store'), { recursive: true, force: true });
          await execute(
            'node',
            [pnpm, 'fetch', '--store-dir', join(directory, 'store')],
            options,
          );
          await execute(
            'node',
            [...installArgs, '--offline', '--frozen-lockfile'],
            options,
          );
          expect(
            await readFile(join(directory, 'pnpm-lock.yaml'), 'utf8'),
          ).toBe(lock);

          // Resolve declaration exports from the fetched artifact for both ESM
          // and CommonJS consumers; source-tree compilation is insufficient.
          const consumer = `
import { Matador, RabbitMQTransport, type MatadorConfig } from '@proone/external-integration-matador';
const startup: Pick<MatadorConfig, 'startupRetry'> = { startupRetry: { retryIntervalMs: 10 } };
export const values = { Matador, RabbitMQTransport, startup };
`;
          await writeFile(join(directory, 'consumer.mts'), consumer);
          await writeFile(join(directory, 'consumer.cts'), consumer);
          await writeFile(
            join(directory, 'tsconfig.json'),
            JSON.stringify({
              compilerOptions: {
                noEmit: true,
                strict: true,
                skipLibCheck: true,
                target: 'ES2022',
                module: 'NodeNext',
                moduleResolution: 'NodeNext',
                types: [],
              },
              files: ['consumer.mts', 'consumer.cts'],
            }),
          );
          await execute(
            'node',
            [
              require.resolve('typescript/bin/tsc'),
              '--project',
              join(directory, 'tsconfig.json'),
            ],
            options,
          );

          await execute(
            'node',
            [
              '--input-type=module',
              '--eval',
              `
        import assert from 'node:assert/strict';
        import { createRequire } from 'node:module';
        import { readFileSync } from 'node:fs';
        import { dirname, join } from 'node:path';
        const require = createRequire(import.meta.url);
        const fixed = require('@proone/external-integration-matador');
        const original = require('@zdavison/matador');
        const esm = await import('@proone/external-integration-matador');
        assert.equal(typeof fixed.Matador, 'function');
        assert.equal(typeof fixed.RabbitMQTransport, 'function');
        assert.equal(typeof esm.Matador, 'function');
        assert.equal(typeof esm.RabbitMQTransport, 'function');
        assert.notEqual(fixed.Matador, original.Matador);
        for (const [name, version] of [
          ['@zdavison/matador', '4.2.0'],
          ['@proone/external-integration-matador', '4.2.0-proone.1']
        ]) {
          const manifest = join(dirname(dirname(require.resolve(name))), 'package.json');
          assert.equal(JSON.parse(readFileSync(manifest, 'utf8')).version, version);
        }
      `,
            ],
            options,
          );
        } finally {
          await rm(directory, { recursive: true, force: true });
        }
      },
      600_000,
    );
  },
);
