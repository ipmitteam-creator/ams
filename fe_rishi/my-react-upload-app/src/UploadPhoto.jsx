
import React, { useState } from 'react';

export default function UploadPhoto() {
  const [badgeNo, setBadgeNo] = useState('');
  const [file, setFile] = useState(null);
  const [status, setStatus] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!file || !badgeNo) {
      setStatus('Badge No and photo are required!');
      return;
    }
    const formData = new FormData();
    formData.append('badge_no', badgeNo);
    formData.append('file', file);
    try {
      const res = await fetch('https://your-api-url/sewadar/upload-photo', {
        method: 'POST',
        body: formData
      });
      if (res.ok) {
        const data = await res.json();
        setStatus(data.message);
      } else {
        const err = await res.json();
        setStatus(`Error: ${err.detail || res.statusText}`);
      }
    } catch (error) {
      setStatus(`Upload failed: ${error.message}`);
    }
  };

  return (
    <div style={{ maxWidth: '400px', margin: '2rem auto', padding: '1rem', border: '1px solid #ccc', borderRadius: '8px' }}>
      <h2>Upload Sewadar Photo</h2>
      <form onSubmit={handleSubmit}>
        <div>
          <label>Badge No</label>
          <input
            type="text"
            value={badgeNo}
            onChange={(e) => setBadgeNo(e.target.value)}
            style={{ display: 'block', width: '100%', marginBottom: '1rem' }}
            required
          />
        </div>
        <div>
          <label>Photo</label>
          <input
            type="file"
            accept="image/*"
            onChange={(e) => setFile(e.target.files[0])}
            style={{ display: 'block', marginBottom: '1rem' }}
            required
          />
        </div>
        <button type="submit" style={{ padding: '0.5rem 1rem', background: '#4f46e5', color: '#fff', border: 'none', borderRadius: '4px' }}>Upload</button>
      </form>
      {status && <p style={{ marginTop: '1rem' }}>{status}</p>}
    </div>
  );
}
