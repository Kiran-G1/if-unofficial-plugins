import * as DATA from './data.json';
import * as REGION_DATA from './region-data.json';

export function getMockResponse(url: string) {
  switch (url) {
    case 'https://api.watttime.org/login':
      if (
        !process.env.WATT_TIME_USERNAME ||
        !process.env.WATT_TIME_PASSWORD ||
        (['ENV_WATT_USERNAME', 'wrong1'].includes(
          process.env.WATT_TIME_USERNAME
        ) &&
          ['ENV_WATT_PASSWORD', 'wrong2'].includes(
            process.env.WATT_TIME_PASSWORD
          ))
      ) {
        return Promise.resolve({
          status: 401,
          data: {},
        });
      }

      return Promise.resolve({
        status: 200,
        data: {
          token: 'test_token',
        },
      });

    case 'https://api2.watttime.org/v2/data':
      if (
        process.env.WATT_TIME_USERNAME === 'invalidData1' &&
        process.env.WATT_TIME_PASSWORD === 'invalidData2'
      ) {
        return Promise.resolve({
          status: 400,
          data: {},
        });
      } else if (
        process.env.WATT_TIME_USERNAME === 'invalidData' &&
        process.env.WATT_TIME_PASSWORD === 'invalidData'
      ) {
        return Promise.resolve({
          status: 200,
          data: {},
        });
      } else if (
        process.env.WATT_TIME_USERNAME === 'fetchError1' &&
        process.env.WATT_TIME_PASSWORD === 'fetchError2'
      ) {
        return Promise.resolve({
          status: 400,
          data: {none: {}},
        });
      } else if (
        process.env.WATT_TIME_USERNAME === 'API_error' &&
        process.env.WATT_TIME_PASSWORD === 'API_error'
      ) {
        return Promise.reject({
          status: 400,
          error: {message: 'error'},
        });
      }

      return Promise.resolve({
        data: DATA,
        status: 200,
      });

    case 'https://api.watttime.org/v3/forecast/historical':
      if (
        process.env.WATT_TIME_USERNAME === 'invalidRegionWT' &&
        process.env.WATT_TIME_PASSWORD === 'invalidRegionWT'
      ) {
        return Promise.reject({
          status: 400,
          error: {message: 'error'},
        });
      } else if (
        process.env.WATT_TIME_USERNAME === 'invalidRegionWT1' &&
        process.env.WATT_TIME_PASSWORD === 'invalidRegionWT1'
      ) {
        return Promise.resolve({
          status: 400,
          data: {},
        });
      } else if (
        process.env.WATT_TIME_USERNAME === 'invalidRegionWT2' &&
        process.env.WATT_TIME_PASSWORD === 'invalidRegionWT2'
      ) {
        return Promise.resolve({
          status: 200,
          data: {},
        });
      }
      return Promise.resolve({
        data: REGION_DATA,
        status: 200,
      });
    case 'https://api.watttime.org/v3/my-access':
      if (
        process.env.WATT_TIME_USERNAME === 'invalidSignalType' &&
        process.env.WATT_TIME_PASSWORD === 'invalidSignalType'
      ) {
        return Promise.reject({
          status: 400,
          error: {message: 'error'},
        });
      } else if (
        process.env.WATT_TIME_USERNAME === 'invalidSignalType1' &&
        process.env.WATT_TIME_PASSWORD === 'invalidSignalType1'
      ) {
        return Promise.resolve({
          status: 400,
          data: {},
        });
      }
      return Promise.resolve({
        data: {
          signal_types: [
            {signal_type: 'co2_moer', regions: []},
            {signal_type: 'health_damage', regions: []},
          ],
        },
        status: 200,
      });
  }
  return Promise.resolve({});
}
