import * as dayjs from 'dayjs';
import {z} from 'zod';

import {ERRORS} from '../../util/errors';
import {buildErrorMessage} from '../../util/helpers';

import {ConfigParams, KeyValuePair, ModelParams} from '../../types/common';
import {PluginInterface} from '../../interfaces';
import {validate} from '../../util/validations';

import {WattAuthType, WattTimeParams} from './types';
import {WattTimeAPI} from './watt-time-api';

const {InputValidationError} = ERRORS;

export const WattTimeGridEmissions = (
  globalConfig: ConfigParams
): PluginInterface => {
  const metadata = {kind: 'execute'};
  const wattTimeAPI = WattTimeAPI();
  const errorBuilder = buildErrorMessage(WattTimeGridEmissions.name);

  /**
   * Initialize authentication with global config.
   */
  const initializeAuthentication = async () => {
    const extractedParams = extractParamsFromConfig();
    const safeConfig: WattAuthType = validateConfig(extractedParams);

    await wattTimeAPI.authenticate(safeConfig);
  };

  /**
   * Calculates the average emission.
   */
  const execute = async (inputs: ModelParams[]): Promise<ModelParams[]> => {
    await initializeAuthentication();
    validateInputs(inputs);

    const wattTimeData = await getWattTimeData(inputs);

    return inputs.map((input, index) => {
      const inputStart = dayjs(input.timestamp);
      const inputEnd = inputStart.add(input.duration, 'seconds');
      const data = getWattTimeDataForDuration(
        wattTimeData,
        inputStart,
        inputEnd
      );

      if (data.length === 0) {
        throw new InputValidationError(
          errorBuilder({
            message: `Did not receive data from WattTime API for the input[${index}] block`,
          })
        );
      }

      const totalEmission = data.reduce((a: number, b: number) => a + b, 0);
      input['grid/carbon-intensity'] = totalEmission / data.length;

      return input;
    });
  };

  /**
   * lbs/MWh to Kg/MWh by dividing by 0.453592 (0.453592 Kg/lbs)
   * (Kg/MWh == g/kWh)
   * convert to kg/KWh by dividing by 1000. (1MWh = 1000KWh)
   * convert to g/KWh by multiplying by 1000. (1Kg = 1000g)
   * hence each other cancel out and g/KWh is the same as kg/MWh
   */
  const getWattTimeDataForDuration = (
    wattTimeData: KeyValuePair[],
    inputStart: dayjs.Dayjs,
    inputEnd: dayjs.Dayjs
  ) => {
    const kgMWh = 0.45359237;

    return wattTimeData.reduce((accumulator, data) => {
      if (
        !dayjs(data.point_time).isBefore(inputStart) &&
        !dayjs(data.point_time).isAfter(inputEnd) &&
        dayjs(data.point_time).format() !== dayjs(inputEnd).format()
      ) {
        accumulator.push(data.value / kgMWh);
      }
      return accumulator;
    }, []);
  };

  /**
   * Validates inputs for geolocation, latitude and longitude.
   */
  const validateInputs = (inputs: ModelParams[]) => {
    inputs.forEach((input, index) => {
      if ('geolocation' in input) {
        const {latitude, longitude} = parseLocation(input);

        if (isNaN(latitude) || isNaN(longitude)) {
          throw new InputValidationError(
            errorBuilder({
              message: `'latitude' or 'longitude' from input[${index}] is not a number`,
            })
          );
        }
      }
    });
  };

  /**
   * Parses the geolocation string from the input data to extract latitude and longitude.
   * Throws an InputValidationError if the geolocation string is invalid.
   */
  const parseLocation = (
    input: ModelParams
  ): {
    latitude: number;
    longitude: number;
  } => {
    const safeInput = Object.assign(input, validateSingleInput(input));
    const [latitude, longitude] = safeInput['geolocation'].split(',');

    return {latitude: parseFloat(latitude), longitude: parseFloat(longitude)};
  };

  /**
   * Validates single input.
   */
  const validateSingleInput = (input: ModelParams) => {
    const schema = z.object({
      geolocation: z
        .string()
        .regex(new RegExp('^\\d{1,3}\\.\\d+,-\\d{1,3}\\.\\d+$'), {
          message:
            "'geolocation' should be a comma separated string of 'latitude' and 'longitude'",
        }),
    });

    return validate<z.infer<typeof schema>>(schema, input);
  };

  /**
   * Retrieves data from the WattTime API based on the provided inputs.
   * Determines the start time and fetch duration from the inputs, and parses the geolocation.
   * Fetches data from the WattTime API for the entire duration and returns the sorted data.
   */
  const getWattTimeData = async (inputs: ModelParams[]) => {
    const {startTime, fetchDuration} = calculateStartDurationTime(inputs);

    const {latitude, longitude} = parseLocation(inputs[0]);

    const params: WattTimeParams = {
      latitude,
      longitude,
      starttime: dayjs(startTime).format('YYYY-MM-DDTHH:mm:ssZ'),
      endtime: dayjs(startTime).add(fetchDuration, 'seconds'),
    };

    return await wattTimeAPI.fetchAndSortData(params);
  };

  /**
   * Calculates the start time and fetch duration based on the provided inputs.
   * Iterates through the inputs to find the earliest start time and latest end time.
   * Calculates the fetch duration based on the time range.
   * Throws an InputValidationError if the fetch duration exceeds the maximum supported by the WattTime API.
   *
   */
  const calculateStartDurationTime = (
    inputs: ModelParams[]
  ): {
    startTime: string;
    fetchDuration: number;
  } => {
    const {startTime, endtime} = inputs.reduce(
      (acc, input) => {
        const {duration, timestamp} = input;
        const dayjsTimestamp = dayjs(timestamp);
        const startTime = dayjsTimestamp.isBefore(acc.startTime)
          ? dayjsTimestamp
          : acc.startTime;
        const durationInSeconds = dayjsTimestamp.add(duration, 'seconds');
        const endTime = durationInSeconds.isAfter(acc.endtime)
          ? durationInSeconds
          : acc.endtime;

        return {startTime: startTime, endtime: endTime};
      },
      {startTime: dayjs('9999-12-31'), endtime: dayjs('1970-01-01')}
    );

    const fetchDuration = endtime.diff(startTime, 'seconds');

    // WattTime API only supports up to 32 days
    if (fetchDuration > 32 * 24 * 60 * 60) {
      throw new InputValidationError(
        errorBuilder({
          message: `WattTime API supports up to 32 days. Duration of ${fetchDuration} seconds is too long`,
        })
      );
    }

    return {startTime: startTime.format(), fetchDuration};
  };

  /**
   * Validates static parameters.
   */
  const validateConfig = (config: ConfigParams) => {
    const schema = z.object({
      username: z.string(),
      password: z.string(),
      token: z.string().optional(),
      baseUrl: z.string().optional(),
    });

    return validate<z.infer<typeof schema>>(schema, config);
  };

  /**
   * Extracts username, password, and token from the provided static parameters.
   * Removes the 'ENV_' prefix from the parameters if present.
   */
  const extractParamsFromConfig = () => {
    const username: string =
      'username' in globalConfig ? (globalConfig['username'] as string) : '';
    const password: string =
      'password' in globalConfig ? (globalConfig['password'] as string) : '';
    const token: string =
      'token' in globalConfig ? (globalConfig['token'] as string) : '';

    [username, password, token].map(item => removeENVPrefix(item));

    return Object.assign({}, globalConfig, username, password, token);
  };

  /**
   * Removes the 'ENV_' prefix from the provided string if present and retrieves the corresponding environment variable value.
   * If the string does not start with 'ENV_', returns an empty string.
   */
  const removeENVPrefix = (item: string) => {
    return item.startsWith('ENV_') && (process.env[item.slice(4)] ?? '');
  };

  return {
    metadata,
    execute,
  };
};
