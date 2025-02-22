import path from 'path';
import fs from 'fs';
import { spawnSync } from 'child_process';

const ANDROID_TARGET_TO_DESTINATION = {
  // 'aarch64-linux-android': 'arm64-v8a',
  'x86_64-linux-android': 'x86_64',
  // 'i686-linux-android': 'x86',
  // 'armv7-linux-androideabi': 'armeabi-v7a',
};

const build_android = (target: string) => {
  console.info('Building rust library for android target: ', target);
  const s3Args = ['--features', 's3_sync'];
  spawnSync(
    'cross',
    ['build', '--target', target, '--release', '-j4'],
    {
      stdio: 'inherit',
    }
  );
};

const main = () => {
  process.chdir('timon');
  Object.keys(ANDROID_TARGET_TO_DESTINATION).forEach(build_android);
  process.chdir('..');

  Object.entries(ANDROID_TARGET_TO_DESTINATION).forEach(([target, architecture]) => {
    console.info('Moving rust library for android target: ', target);
    const sourcePath = path.join( // Ensure the path matches the library location on your filesystem
      process.cwd(),
      'timon',
      'target',
      target,
      'release',
      'libtsdb_timon.so'
    );
    const architecturePath = path.join( // Ensure the path matches the library location on your filesystem
      process.cwd(),
      'android',
      'app',
      'src',
      'main',
      'jniLibs',
      architecture
    );
    if (!fs.existsSync(architecturePath)) {
      fs.mkdirSync(architecturePath, { recursive: true });
    }
    fs.copyFileSync(
      sourcePath,
      path.join(architecturePath, 'libtsdb_timon.so')
    );
  });
};

main();
