import React, { useEffect, useState } from 'react'
import '../App.css';

export default function AppStats() {
    const [isLoaded, setIsLoaded] = useState(false);
    const [stats, setStats] = useState({});
    const [error, setError] = useState(null)

	const getStats = () => {
	
        fetch(`http://njanus.eastus2.cloudapp.azure.com/processing/stats`)
            .then(res => res.json())
            .then((result)=>{
				console.log("Received Stats")
                setStats(result);
                setIsLoaded(true);
            },(error) =>{
                setError(error)
                setIsLoaded(true);
            })
    }
    useEffect(() => {
		const interval = setInterval(() => getStats(), 2000); // Update every 2 seconds
		return() => clearInterval(interval);
    }, [getStats]);

    if (error){
        return (<div className={"error"}>Error found when fetching from API</div>)
    } else if (isLoaded === false){
        return(<div>Loading...</div>)
    } else if (isLoaded === true){
        return(
            <div>
                <h1>Latest Stats</h1>
                <table className={"StatsTable"}>
					<tbody>
						<tr>
							<th>Location</th>
							<th>Waypoint</th>
						</tr>
						<tr>
							<td># LOCATION: {stats['num_location_readings']}</td>
							<td># WAYPOINT: {stats['num_waypoint_readings']}</td>
						</tr>
						<tr>
							<td colspan="2">Max Latitude: {stats['max_latitude']}</td>
						</tr>
                        <tr>
							<td colspan="2">Min Latitude: {stats['min_latitude']}</td>
						</tr>
						<tr>
							<td colspan="2">Max Longitude: {stats['max_longitude']}</td>
						</tr>
						<tr>
							<td colspan="2">Min Longitude: {stats['min_longitude']}</td>
						</tr>
					</tbody>
                </table>
                <h3>Last Updated: {stats['last_updated']}</h3>

            </div>
        )
    }
}
